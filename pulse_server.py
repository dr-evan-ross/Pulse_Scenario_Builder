"""
Pulse Server - REST API for Pulse Physiology Engine

This server runs the Pulse engine and exposes a REST API for:
- Single patient simulations
- Batch simulations
- Ventilator mechanics simulations
- HTTP controller support

The GUI is served separately by pulse_gui.py

Usage:
    pip install flask requests flask-cors
    python pulse_server.py [--port 8080] [--host 0.0.0.0]
"""

import os
import sys
import json
import threading
import queue
import uuid
import time
import io
import csv
import tempfile
import zipfile
import argparse
import requests as http_requests
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# === PATH SETUP ===
PULSE_HOME = os.path.join(os.path.dirname(os.path.abspath(__file__)),"pulse_engine")
PULSE_BIN = os.path.join(PULSE_HOME, "bin")
PULSE_PYTHON = os.path.join(PULSE_HOME, "python")

sys.path.insert(0, PULSE_PYTHON)
sys.path.insert(0, PULSE_BIN)
os.add_dll_directory(PULSE_BIN)

# === PULSE IMPORTS ===
from pulse.engine.PulseEngine import PulseEngine, eModelType
from pulse.engine.PulseEnginePool import PulseEnginePool
from pulse.cdm.engine import SEDataRequest, SEDataRequestManager, SEAdvanceTime, IEventHandler, SEEventChange, eEvent
from multiprocessing import Pool as ProcessPool, TimeoutError as MPTimeoutError
from pulse.cdm.patient_actions import (
    SEIntubation, eIntubationType, 
    SEAcuteRespiratoryDistressSyndromeExacerbation,
    SESubstanceBolus, eSubstance_Administration,
    SESubstanceInfusion,
    SESubstanceCompoundInfusion,
    SEExercise,
    SEAcuteStress,
    SEHemorrhage, eHemorrhage_Compartment,
    SEAirwayObstruction,
    SERespiratoryMechanicsConfiguration
)
from pulse.cdm.physiology import eLungCompartment
from pulse.cdm.mechanical_ventilator_actions import (
    SEMechanicalVentilatorVolumeControl,
    SEMechanicalVentilatorPressureControl,
    eMechanicalVentilator_VolumeControlMode,
    eMechanicalVentilator_PressureControlMode
)
from pulse.cdm.mechanical_ventilator import eSwitch
from pulse.cdm.scalars import (
    FrequencyUnit, PressureUnit, TimeUnit,
    VolumeUnit, VolumePerTimeUnit, MassPerVolumeUnit,
    LengthUnit, MassUnit, PressureTimePerVolumeUnit, VolumePerPressureUnit,
    AmountPerVolumeUnit
)
from pulse.cdm.patient import eSex, SEPatient, SEPatientConfiguration
from pulse.cdm.io.patient import serialize_patient_from_file

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Use absolute paths so they work even after os.chdir()
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(SCRIPT_DIR, 'uploads')
RESULTS_FOLDER = os.path.join(SCRIPT_DIR, 'results')
Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path(RESULTS_FOLDER).mkdir(exist_ok=True)

jobs = {}
job_lock = threading.Lock()
cancel_flags = {}  # job_id -> True if should cancel
pulse_config_lock = threading.Lock()

# Live data streaming
live_data_subscribers = {}  # job_id -> set of variables to stream
live_data_lock = threading.Lock()


@app.after_request
def add_no_cache_headers(response):
    """Prevent browser caching during development."""
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response


# === UNIT MAPPING FOR HTTP CONTROLLERS ===
UNIT_MAP = {
    # Pressure units
    "mmHg": PressureUnit.mmHg,
    "cmH2O": PressureUnit.cmH2O,
    "Pa": PressureUnit.Pa,
    "atm": PressureUnit.atm,
    # Volume units
    "mL": VolumeUnit.mL,
    "L": VolumeUnit.L,
    # Flow units
    "mL/min": VolumePerTimeUnit.mL_Per_min,
    "L/min": VolumePerTimeUnit.L_Per_min,
    "L/s": VolumePerTimeUnit.L_Per_s,
    # Frequency units
    "1/min": FrequencyUnit.Per_min,
    "/min": FrequencyUnit.Per_min,
    "Per_min": FrequencyUnit.Per_min,
    "Hz": FrequencyUnit.Hz,
    "1/s": FrequencyUnit.Per_s,
    # Time units
    "s": TimeUnit.s,
    "min": TimeUnit.min,
    "hr": TimeUnit.hr,
    # Mass units
    "kg": MassUnit.kg,
    "g": MassUnit.g,
    "mg": MassUnit.mg,
    "ug": MassUnit.ug,
    # Length units
    "m": LengthUnit.m,
    "cm": LengthUnit.cm,
    "mm": LengthUnit.mm,
    "inch": LengthUnit.inch,
    # Concentration units
    "g/L": MassPerVolumeUnit.g_Per_L,
    "mg/mL": MassPerVolumeUnit.g_Per_L,
    "ug/mL": MassPerVolumeUnit.ug_Per_mL,
    # Compliance units
    "mL/cmH2O": VolumePerPressureUnit.mL_Per_cmH2O,
    "L/cmH2O": VolumePerPressureUnit.L_Per_cmH2O,
    # Resistance units
    "cmH2O*s/L": PressureTimePerVolumeUnit.cmH2O_s_Per_L,
}

DATA_REQUEST_FACTORIES = {
    "Physiology": SEDataRequest.create_physiology_request,
    "MechanicalVentilator": SEDataRequest.create_mechanical_ventilator_request,
}


# === AVAILABLE CSV OUTPUT VARIABLES ===
# Each entry defines a Pulse data request that can be selected for CSV output.
# "key" = CSV column name, "pulse_name" = Pulse engine variable name
# "request_type": "physiology", "mechanical_ventilator", or "compartment_substance"
# "transform": post-processing applied to raw Pulse value before CSV output
# Variables with "default": True match the original hardcoded set.

AVAILABLE_VARIABLES = [
    # --- Vital Signs ---
    {"key": "hr_bpm", "label": "Heart Rate", "unit": "bpm", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "HeartRate",
     "pulse_unit": "FrequencyUnit.Per_min", "transform": None},
    {"key": "spo2_pct", "label": "SpO2", "unit": "%", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "OxygenSaturation",
     "pulse_unit": None, "transform": "multiply_100"},
    {"key": "etco2_mmhg", "label": "EtCO2", "unit": "mmHg", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "EndTidalCarbonDioxidePressure",
     "pulse_unit": "PressureUnit.mmHg", "transform": None},
    {"key": "rr_patient", "label": "Resp Rate (patient)", "unit": "bpm", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "RespirationRate",
     "pulse_unit": "FrequencyUnit.Per_min", "transform": None},
    {"key": "sbp_mmhg", "label": "Systolic BP", "unit": "mmHg", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "SystolicArterialPressure",
     "pulse_unit": "PressureUnit.mmHg", "transform": None},
    {"key": "dbp_mmhg", "label": "Diastolic BP", "unit": "mmHg", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "DiastolicArterialPressure",
     "pulse_unit": "PressureUnit.mmHg", "transform": None},
    {"key": "map_mmhg", "label": "Mean Arterial Pressure", "unit": "mmHg", "category": "Vital Signs",
     "default": True, "request_type": "physiology", "pulse_name": "MeanArterialPressure",
     "pulse_unit": "PressureUnit.mmHg", "transform": None},

    # --- Blood Gas ---
    {"key": "pao2_mmhg", "label": "PaO2", "unit": "mmHg", "category": "Blood Gas",
     "default": True, "request_type": "physiology", "pulse_name": "ArterialOxygenPressure",
     "pulse_unit": "PressureUnit.mmHg", "transform": None},
    {"key": "paco2_mmhg", "label": "PaCO2", "unit": "mmHg", "category": "Blood Gas",
     "default": True, "request_type": "physiology", "pulse_name": "ArterialCarbonDioxidePressure",
     "pulse_unit": "PressureUnit.mmHg", "transform": None},
    {"key": "ph", "label": "Blood pH", "unit": "", "category": "Blood Gas",
     "default": True, "request_type": "physiology", "pulse_name": "BloodPH",
     "pulse_unit": None, "transform": None},

    # --- Cardiac ---
    {"key": "vt_patient_ml", "label": "Tidal Volume (patient)", "unit": "mL", "category": "Cardiac",
     "default": True, "request_type": "physiology", "pulse_name": "TidalVolume",
     "pulse_unit": "VolumeUnit.mL", "transform": None},
    {"key": "co_lpm", "label": "Cardiac Output", "unit": "L/min", "category": "Cardiac",
     "default": True, "request_type": "physiology", "pulse_name": "CardiacOutput",
     "pulse_unit": "VolumePerTimeUnit.L_Per_min", "transform": None},

    # --- Hematology (DEFAULT in PSB for shock/resuscitation monitoring) ---
    {"key": "lactate_mmol_L", "label": "Lactate", "unit": "mmol/L", "category": "Hematology",
     "default": True, "request_type": "compartment_substance",
     "pulse_name": "Lactate", "pulse_unit": "MassPerVolumeUnit.g_Per_L",
     "compartment": "Aorta", "property": "Concentration",
     "transform": "lactate_g_to_mmol"},
    {"key": "blood_volume_ml", "label": "Blood Volume", "unit": "mL", "category": "Hematology",
     "default": True, "request_type": "physiology", "pulse_name": "BloodVolume",
     "pulse_unit": "VolumeUnit.mL", "transform": None},
    {"key": "hematocrit_pct", "label": "Hematocrit", "unit": "%", "category": "Hematology",
     "default": False, "request_type": "physiology", "pulse_name": "Hematocrit",
     "pulse_unit": None, "transform": None},

    # --- Ventilator (measured) ---
    {"key": "rr_vent", "label": "Resp Rate (vent)", "unit": "bpm", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "RespirationRate",
     "pulse_unit": "FrequencyUnit.Per_min", "transform": None},
    {"key": "vt_vent_ml", "label": "Tidal Volume (vent)", "unit": "mL", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "TidalVolume",
     "pulse_unit": "VolumeUnit.mL", "transform": None},
    {"key": "pip_cmh2o", "label": "Peak Insp Pressure", "unit": "cmH2O", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "PeakInspiratoryPressure",
     "pulse_unit": "PressureUnit.cmH2O", "transform": None},
    {"key": "pplat_cmh2o", "label": "Plateau Pressure", "unit": "cmH2O", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "PlateauPressure",
     "pulse_unit": "PressureUnit.cmH2O", "transform": None},
    {"key": "paw_mean_cmh2o", "label": "Mean Airway Pressure", "unit": "cmH2O", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "MeanAirwayPressure",
     "pulse_unit": "PressureUnit.cmH2O", "transform": None},
    {"key": "insp_flow_lpm", "label": "Inspiratory Flow", "unit": "L/min", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "InspiratoryFlow",
     "pulse_unit": "VolumePerTimeUnit.L_Per_min", "transform": None},
    {"key": "exp_flow_lpm", "label": "Expiratory Flow", "unit": "L/min", "category": "Ventilator",
     "default": True, "request_type": "mechanical_ventilator", "pulse_name": "ExpiratoryFlow",
     "pulse_unit": "VolumePerTimeUnit.L_Per_min", "transform": None},

    # --- Respiratory Mechanics (non-default) ---
    {"key": "resp_compliance_ml_cmh2o", "label": "Respiratory Compliance", "unit": "mL/cmH2O",
     "category": "Respiratory Mechanics", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "DynamicRespiratoryCompliance",
     "pulse_unit": "VolumePerPressureUnit.mL_Per_cmH2O", "transform": None},
    {"key": "static_compliance_ml_cmh2o", "label": "Static Compliance", "unit": "mL/cmH2O",
     "category": "Respiratory Mechanics", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "StaticRespiratoryCompliance",
     "pulse_unit": "VolumePerPressureUnit.mL_Per_cmH2O", "transform": None},

    # --- Additional Ventilator (non-default) ---
    {"key": "airway_pressure_cmh2o", "label": "Airway Pressure", "unit": "cmH2O",
     "category": "Additional Ventilator", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "AirwayPressure", "pulse_unit": "PressureUnit.cmH2O", "transform": None},
    {"key": "total_peep_cmh2o", "label": "Total PEEP", "unit": "cmH2O",
     "category": "Additional Ventilator", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "TotalPositiveEndExpiratoryPressure",
     "pulse_unit": "PressureUnit.cmH2O", "transform": None},
    {"key": "ie_ratio", "label": "I:E Ratio", "unit": "",
     "category": "Additional Ventilator", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "InspiratoryExpiratoryRatio", "pulse_unit": None, "transform": None},
    {"key": "insp_vt_ml", "label": "Inspiratory Vt", "unit": "mL",
     "category": "Additional Ventilator", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "InspiratoryTidalVolume", "pulse_unit": "VolumeUnit.mL", "transform": None},
    {"key": "exp_vt_ml", "label": "Expiratory Vt", "unit": "mL",
     "category": "Additional Ventilator", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "ExpiratoryTidalVolume", "pulse_unit": "VolumeUnit.mL", "transform": None},
    {"key": "peak_insp_flow_lpm", "label": "Peak Insp Flow", "unit": "L/min",
     "category": "Additional Ventilator", "default": False, "request_type": "mechanical_ventilator",
     "pulse_name": "PeakInspiratoryFlow", "pulse_unit": "VolumePerTimeUnit.L_Per_min", "transform": None},

    # --- Temperature (non-default) ---
    {"key": "skin_temp_c", "label": "Skin Temperature", "unit": "C",
     "category": "Temperature", "default": False, "request_type": "physiology",
     "pulse_name": "SkinTemperature", "pulse_unit": None, "transform": None},

    # --- Advanced Respiratory (non-default) ---
    {"key": "total_lung_volume_ml", "label": "Total Lung Volume", "unit": "mL",
     "category": "Advanced Respiratory", "default": False, "request_type": "physiology",
     "pulse_name": "TotalLungVolume", "pulse_unit": "VolumeUnit.mL", "transform": None},
    {"key": "total_pulm_ventilation_lpm", "label": "Total Pulmonary Ventilation", "unit": "L/min",
     "category": "Advanced Respiratory", "default": False, "request_type": "physiology",
     "pulse_name": "TotalPulmonaryVentilation", "pulse_unit": "VolumePerTimeUnit.L_Per_min", "transform": None},
]

# Columns always included in CSV (not Pulse data requests - internally tracked state)
# PSB includes fluid controller columns
ALWAYS_INCLUDED_COLUMNS = [
    "cmd_mode", "cmd_vt_ml", "cmd_rr", "cmd_fio2",
    "cmd_peep_cmh2o", "cmd_pinsp_cmh2o", "cmd_itime_s",
    "is_intubated", "vent_active", "controller_active", "fluid_controller_active",
    "blood_loss_ml", "blood_infused_ml", "crystalloid_infused_ml",
    "cmd_crystalloid_rate", "cmd_blood_rate",
    "event", "controller_cmd", "fluid_cmd"
]

# Default variable keys (for backward compatibility when no output_columns specified)
DEFAULT_OUTPUT_KEYS = [v["key"] for v in AVAILABLE_VARIABLES if v["default"]]

# Unit string -> actual Pulse unit object mapping
PULSE_UNIT_MAP = {
    "FrequencyUnit.Per_min": FrequencyUnit.Per_min,
    "PressureUnit.mmHg": PressureUnit.mmHg,
    "PressureUnit.cmH2O": PressureUnit.cmH2O,
    "VolumeUnit.mL": VolumeUnit.mL,
    "VolumePerTimeUnit.L_Per_min": VolumePerTimeUnit.L_Per_min,
    "VolumePerTimeUnit.mL_Per_min": VolumePerTimeUnit.mL_Per_min,
    "MassPerVolumeUnit.g_Per_L": MassPerVolumeUnit.g_Per_L,
    "VolumePerPressureUnit.mL_Per_cmH2O": VolumePerPressureUnit.mL_Per_cmH2O,
    "PressureTimePerVolumeUnit.cmH2O_s_Per_L": PressureTimePerVolumeUnit.cmH2O_s_Per_L,
}


def resolve_selected_vars(output_columns=None):
    """Return the list of AVAILABLE_VARIABLES entries matching the selected keys.
    If output_columns is None, returns the default set. Preserves registry order."""
    if output_columns is None:
        return [v for v in AVAILABLE_VARIABLES if v["default"]]
    selected_set = set(output_columns)
    return [v for v in AVAILABLE_VARIABLES if v["key"] in selected_set]


def build_data_requests(selected_vars):
    """Build Pulse SEDataRequest list from selected variable definitions."""
    data_requests = []
    for var in selected_vars:
        unit = PULSE_UNIT_MAP.get(var.get("pulse_unit")) if var.get("pulse_unit") else None
        kwargs = {}
        if unit is not None:
            kwargs["unit"] = unit

        if var["request_type"] == "physiology":
            data_requests.append(SEDataRequest.create_physiology_request(var["pulse_name"], **kwargs))
        elif var["request_type"] == "mechanical_ventilator":
            data_requests.append(SEDataRequest.create_mechanical_ventilator_request(var["pulse_name"], **kwargs))
        elif var["request_type"] == "compartment_substance":
            data_requests.append(
                SEDataRequest.create_liquid_compartment_substance_request(
                    var["compartment"], var["pulse_name"], var["property"], **kwargs
                )
            )
    return data_requests


def build_vitals_dict(results, selected_vars):
    """Convert Pulse results array to a named vitals dict based on selected variables.
    results[0] is always sim_time, results[1..N] correspond to selected_vars in order."""
    vitals = {"t": results[0]}
    for i, var in enumerate(selected_vars):
        val = results[i + 1]
        transform = var.get("transform")
        if transform == "multiply_100":
            val = val * 100
        elif transform == "lactate_g_to_mmol":
            val = val * 11.1  # g/L to mmol/L (MW lactate ~90.08)
        vitals[var["key"]] = val
    return vitals


def build_csv_columns(selected_vars):
    """Build the CSV column header list from selected variables plus always-included columns."""
    cols = ["sim_time_s"]
    cols.extend([v["key"] for v in selected_vars])
    cols.extend(ALWAYS_INCLUDED_COLUMNS)
    return cols


class HTTPController:
    """HTTP-based controller that communicates with an external controller service."""

    def __init__(self, base_url, config=None, timeout=10.0, simulation_context=None):
        self.base_url = base_url.rstrip('/')
        self.config = config or {}
        self.timeout = timeout
        self.data_requests = []
        self.next_update_s = 1.0
        self.initialized = False
        self.last_error = None
        # Simulation context for identification (single-patient mode uses job_id)
        self.simulation_context = simulation_context or {}
        self.simulation_id = self.simulation_context.get('simulation_id', '')
        self.job_id = self.simulation_context.get('job_id', '')

    def send_init(self, patient, vent_settings):
        payload = {
            "patient": patient,
            "vent_settings": vent_settings,
            "config": self.config,
            # Simulation identifiers for consistency with batch mode
            "simulation_id": self.simulation_id,
            "job_id": self.job_id
        }
        
        try:
            resp = http_requests.post(
                f"{self.base_url}/init",
                json=payload,
                timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("status") != "ok":
                raise RuntimeError(f"Controller init failed: {data.get('error', 'unknown error')}")
            
            self.data_requests = data.get("data_requests", [])
            pulse_requests = []
            
            for req in self.data_requests:
                category = req.get("category")
                name = req.get("name")
                unit_str = req.get("unit")
                
                if category not in DATA_REQUEST_FACTORIES:
                    raise ValueError(f"Unknown data request category: '{category}'")
                
                factory = DATA_REQUEST_FACTORIES[category]
                
                if unit_str:
                    if unit_str not in UNIT_MAP:
                        raise ValueError(f"Unknown unit: '{unit_str}' for {category}:{name}")
                    unit = UNIT_MAP[unit_str]
                    pulse_requests.append(factory(name, unit=unit))
                else:
                    pulse_requests.append(factory(name))
            
            self.next_update_s = data.get("next_update_s", 1.0)
            self.initialized = True
            self.last_error = None
            
            return pulse_requests
            
        except http_requests.exceptions.RequestException as e:
            self.last_error = f"HTTP error during init: {e}"
            raise RuntimeError(self.last_error)
        except (KeyError, ValueError) as e:
            self.last_error = f"Invalid controller response: {e}"
            raise RuntimeError(self.last_error)
    
    def step(self, data_values, current_settings):
        if not self.initialized:
            return None

        sim_time = data_values.pop("sim_time_s", 0.0)

        payload = {
            "sim_time_s": sim_time,
            "data": data_values,
            # Simulation identifiers for consistency with batch mode
            "simulation_id": self.simulation_id,
            "job_id": self.job_id
        }

        try:
            resp = http_requests.post(
                f"{self.base_url}/update",
                json=payload,
                timeout=self.timeout
            )
            resp.raise_for_status()
            response = resp.json()
            
            if "next_update_s" in response:
                self.next_update_s = max(0.02, response["next_update_s"])
            
            commands = response.get("commands", {})
            result = current_settings.copy()
            result.update(commands)
            result["next_interval_s"] = self.next_update_s
            
            self.last_error = None
            return result
            
        except http_requests.exceptions.Timeout:
            self.last_error = f"Controller timeout after {self.timeout}s"
            print(f"[HTTPController] {self.last_error}")
            return None
        except http_requests.exceptions.RequestException as e:
            self.last_error = f"HTTP error: {e}"
            print(f"[HTTPController] {self.last_error}")
            return None
        except Exception as e:
            self.last_error = f"Error parsing response: {e}"
            print(f"[HTTPController] {self.last_error}")
            return None
    
    def shutdown(self):
        if not self.initialized:
            return
        try:
            payload = {
                "simulation_id": self.simulation_id,
                "job_id": self.job_id
            }
            http_requests.post(f"{self.base_url}/shutdown", json=payload, timeout=2.0)
        except:
            pass


class HTTPFluidController:
    """HTTP-based fluid controller that communicates with an external controller service."""

    def __init__(self, base_url, config=None, timeout=10.0, simulation_context=None):
        self.base_url = base_url.rstrip('/')
        self.config = config or {}
        self.timeout = timeout
        self.data_requests = []
        self.next_update_s = 10.0
        self.initialized = False
        self.last_error = None
        # Simulation context for identification
        self.simulation_context = simulation_context or {}
        self.simulation_id = self.simulation_context.get('simulation_id', '')
        self.job_id = self.simulation_context.get('job_id', '')

    def send_init(self, patient, fluid_settings):
        payload = {
            "patient": patient,
            "fluid_settings": fluid_settings,
            "config": self.config,
            "simulation_id": self.simulation_id,
            "job_id": self.job_id
        }

        try:
            resp = http_requests.post(
                f"{self.base_url}/init",
                json=payload,
                timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "ok":
                raise RuntimeError(f"Fluid controller init failed: {data.get('error', 'unknown error')}")

            self.data_requests = data.get("data_requests", [])
            self.next_update_s = data.get("next_update_s", 10.0)
            self.initialized = True
            self.last_error = None

            return self.data_requests

        except http_requests.exceptions.RequestException as e:
            self.last_error = f"HTTP error during init: {e}"
            raise RuntimeError(self.last_error)
        except (KeyError, ValueError) as e:
            self.last_error = f"Invalid controller response: {e}"
            raise RuntimeError(self.last_error)

    def step(self, vitals, current_settings, blood_loss_ml=0, blood_infused_ml=0, crystalloid_infused_ml=0):
        if not self.initialized:
            return None

        sim_time = vitals.pop("sim_time_s", 0.0) if "sim_time_s" in vitals else 0.0

        payload = {
            "sim_time_s": sim_time,
            "data": vitals,
            "blood_loss_ml": blood_loss_ml,
            "blood_infused_ml": blood_infused_ml,
            "crystalloid_infused_ml": crystalloid_infused_ml,
            "simulation_id": self.simulation_id,
            "job_id": self.job_id
        }

        try:
            resp = http_requests.post(
                f"{self.base_url}/update",
                json=payload,
                timeout=self.timeout
            )
            resp.raise_for_status()
            response = resp.json()

            if "next_update_s" in response:
                self.next_update_s = max(1.0, response["next_update_s"])

            commands = response.get("commands", {})
            result = current_settings.copy()
            result.update(commands)
            result["next_interval_s"] = self.next_update_s

            self.last_error = None
            return result

        except http_requests.exceptions.Timeout:
            self.last_error = f"Fluid controller timeout after {self.timeout}s"
            print(f"[HTTPFluidController] {self.last_error}")
            return None
        except http_requests.exceptions.RequestException as e:
            self.last_error = f"HTTP error: {e}"
            print(f"[HTTPFluidController] {self.last_error}")
            return None
        except Exception as e:
            self.last_error = f"Error parsing response: {e}"
            print(f"[HTTPFluidController] {self.last_error}")
            return None

    def shutdown(self):
        if not self.initialized:
            return
        try:
            payload = {
                "simulation_id": self.simulation_id,
                "job_id": self.job_id
            }
            http_requests.post(f"{self.base_url}/shutdown", json=payload, timeout=2.0)
        except:
            pass


class BuiltinController:
    """Built-in controller implementations."""

    # Random walk parameter bounds (matching reference_controllers/random_controller.py)
    RANDOM_WALK_BOUNDS = {
        'fio2': {'min': 0.21, 'max': 1.0, 'step': 0.05, 'default': 0.4},
        'peep_cmh2o': {'min': 5, 'max': 20, 'step': 2, 'default': 5},
        'vt_ml': {'min': 300, 'max': 600, 'step': 25, 'default': 420},
        'rr': {'min': 10, 'max': 30, 'step': 2, 'default': 14},
        'itime_s': {'min': 0.8, 'max': 1.5, 'step': 0.1, 'default': 1.0},
        'pinsp_cmh2o': {'min': 10, 'max': 30, 'step': 2, 'default': 15},
    }

    def __init__(self, name):
        self.name = name
        self.settings = {}
        self.state = {'history': []}
        self.rng = None  # For random walk controller
    
    def send_init(self, patient, settings):
        self.settings = settings.copy()
        self.state = {'history': [], 'patient': patient}
        # Initialize RNG for random walk controller
        if self.name == 'random_walk_controller':
            import random
            self.rng = random.Random()  # Unseeded for variability each run
    
    def step(self, vitals, current_settings):
        self.settings = current_settings.copy()

        if self.name == 'default_controller':
            return self._simple_fio2(vitals)
        elif self.name == 'ardsnet_controller':
            return self._ardsnet(vitals)
        elif self.name == 'adaptive_controller':
            return self._adaptive(vitals)
        elif self.name == 'random_walk_controller':
            return self._random_walk(vitals)
        return None
    
    def _simple_fio2(self, vitals):
        new = self.settings.copy()
        spo2 = vitals['spo2_pct']
        fio2 = self.settings.get('fio2', 0.4)
        
        if spo2 < 92:
            new['fio2'] = min(1.0, fio2 + 0.05)
        elif spo2 > 98:
            new['fio2'] = max(0.21, fio2 - 0.05)
        return new
    
    def _ardsnet(self, vitals):
        new = self.settings.copy()
        spo2 = vitals['spo2_pct']
        ph = vitals['ph']
        pplat = vitals['pplat_cmh2o']
        
        if spo2 < 88:
            new['fio2'] = min(1.0, self.settings.get('fio2', 0.4) + 0.1)
            new['peep_cmh2o'] = min(20, self.settings.get('peep_cmh2o', 5) + 2)
        elif spo2 > 95:
            new['fio2'] = max(0.3, self.settings.get('fio2', 0.4) - 0.05)
        
        if ph < 7.30:
            new['rr'] = min(35, self.settings.get('rr', 14) + 2)
        elif ph > 7.45:
            new['rr'] = max(6, self.settings.get('rr', 14) - 2)
        
        if pplat > 30:
            new['vt_ml'] = max(300, self.settings.get('vt_ml', 420) - 20)
        
        return new
    
    def _adaptive(self, vitals):
        new = self._simple_fio2(vitals)
        spo2 = vitals['spo2_pct']

        if spo2 < 88:
            new['next_interval_s'] = 0.5
        elif spo2 < 92 or spo2 > 98:
            new['next_interval_s'] = 1.0
        else:
            new['next_interval_s'] = 5.0

        return new

    def _random_walk(self, vitals):
        """
        Bounded random walk controller - ignores vitals, randomly walks all parameters.
        Useful as a control arm to compare against intelligent controllers.
        """
        new = self.settings.copy()

        for param, bounds in self.RANDOM_WALK_BOUNDS.items():
            current = self.settings.get(param, bounds['default'])
            step = bounds['step']

            # Random direction: -1, 0, or +1
            direction = self.rng.choice([-1, 0, 1])
            new_value = current + (direction * step)

            # Clamp to bounds
            new_value = max(bounds['min'], min(bounds['max'], new_value))

            # Round appropriately
            if isinstance(bounds['default'], float):
                new_value = round(new_value, 2)
            else:
                new_value = int(round(new_value))

            new[param] = new_value

        # Random interval for next update (10-60s like the reference controller)
        new['next_interval_s'] = self.rng.uniform(10, 60)

        return new


class BuiltinFluidController:
    """Built-in fluid resuscitation controller implementations.

    Manages crystalloid and blood product infusions based on hemodynamic status.
    Designed to work independently of ventilator controllers.
    """

    def __init__(self, name):
        self.name = name
        self.settings = {
            'crystalloid_rate_ml_min': 0,
            'blood_rate_ml_min': 0,
            'crystalloid_compound': 'Saline',
            'blood_compound': 'Blood'
        }
        self.state = {
            'phase': 'monitoring',  # monitoring, resuscitating, maintaining
            'total_crystalloid_given': 0,
            'total_blood_given': 0,
            'last_map': None,
            'last_hr': None,
            'trend_improving': False
        }

    def send_init(self, patient, initial_settings=None):
        """Initialize controller with patient info and optional settings."""
        self.state['patient'] = patient
        if initial_settings:
            self.settings.update(initial_settings)

    def step(self, vitals, current_fluid_settings, blood_loss_ml=0, blood_infused_ml=0, crystalloid_infused_ml=0):
        """Evaluate hemodynamics and return fluid commands.

        Args:
            vitals: Dict with hr_bpm, map_mmhg, sbp_mmhg, etc.
            current_fluid_settings: Current infusion rates
            blood_loss_ml: Cumulative blood loss
            blood_infused_ml: Cumulative blood products given
            crystalloid_infused_ml: Cumulative crystalloid given

        Returns:
            Dict with fluid commands and next_interval_s
        """
        if self.name == 'default_fluid_controller':
            return self._simple_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
        elif self.name == 'aggressive_fluid_controller':
            return self._aggressive_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
        elif self.name == 'conservative_fluid_controller':
            return self._conservative_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
        elif self.name == 'damage_control_fluid_controller':
            return self._damage_control_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
        return None

    def _simple_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
        """Simple MAP-based fluid resuscitation.

        Strategy:
        - MAP < 65: Start/increase crystalloid
        - MAP < 55: Add blood products
        - MAP > 75: Reduce infusion rates
        - MAP 65-75: Maintain current rates
        """
        new = self.settings.copy()
        map_mmhg = vitals.get('map_mmhg', 70)
        hr_bpm = vitals.get('hr_bpm', 80)

        # Track trends
        if self.state['last_map'] is not None:
            self.state['trend_improving'] = map_mmhg > self.state['last_map']
        self.state['last_map'] = map_mmhg
        self.state['last_hr'] = hr_bpm

        # Determine phase and action
        if map_mmhg < 55:
            # Severe hypotension - aggressive resuscitation
            self.state['phase'] = 'resuscitating'
            new['crystalloid_rate_ml_min'] = 250  # ~15 L/hr max
            # Add blood if significant blood loss
            if blood_loss_ml > 500:
                new['blood_rate_ml_min'] = 150
            else:
                new['blood_rate_ml_min'] = 0
            new['next_interval_s'] = 10  # Check frequently

        elif map_mmhg < 65:
            # Moderate hypotension
            self.state['phase'] = 'resuscitating'
            current_crystalloid = self.settings.get('crystalloid_rate_ml_min', 0)
            new['crystalloid_rate_ml_min'] = min(200, current_crystalloid + 50)
            # Blood if large loss and not keeping up
            if blood_loss_ml > 750 and (blood_loss_ml - blood_infused_ml - crystalloid_infused_ml/3) > 500:
                new['blood_rate_ml_min'] = 100
            else:
                new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 25)
            new['next_interval_s'] = 15

        elif map_mmhg > 75:
            # Adequate perfusion - reduce rates
            self.state['phase'] = 'maintaining'
            current_crystalloid = self.settings.get('crystalloid_rate_ml_min', 0)
            current_blood = self.settings.get('blood_rate_ml_min', 0)
            new['crystalloid_rate_ml_min'] = max(0, current_crystalloid - 50)
            new['blood_rate_ml_min'] = max(0, current_blood - 50)
            new['next_interval_s'] = 30

        else:
            # MAP 65-75: Stable, gradual reduction
            self.state['phase'] = 'maintaining'
            current_crystalloid = self.settings.get('crystalloid_rate_ml_min', 0)
            new['crystalloid_rate_ml_min'] = max(0, current_crystalloid - 25)
            new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 25)
            new['next_interval_s'] = 20

        # Tachycardia as secondary indicator (compensatory shock)
        if hr_bpm > 120 and map_mmhg < 70:
            new['crystalloid_rate_ml_min'] = max(new['crystalloid_rate_ml_min'], 150)
            new['next_interval_s'] = min(new['next_interval_s'], 15)

        self.settings = new.copy()
        return new

    def _aggressive_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
        """Aggressive fluid resuscitation - higher rates, earlier blood."""
        new = self.settings.copy()
        map_mmhg = vitals.get('map_mmhg', 70)
        hr_bpm = vitals.get('hr_bpm', 80)

        if map_mmhg < 60:
            new['crystalloid_rate_ml_min'] = 300
            new['blood_rate_ml_min'] = 200 if blood_loss_ml > 300 else 100
            new['next_interval_s'] = 5
        elif map_mmhg < 70:
            new['crystalloid_rate_ml_min'] = 200
            new['blood_rate_ml_min'] = 100 if blood_loss_ml > 500 else 0
            new['next_interval_s'] = 10
        elif map_mmhg > 80:
            new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 100)
            new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 100)
            new['next_interval_s'] = 30
        else:
            new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 50)
            new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
            new['next_interval_s'] = 20

        self.settings = new.copy()
        return new

    def _conservative_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
        """Conservative/permissive hypotension approach - lower MAP targets."""
        new = self.settings.copy()
        map_mmhg = vitals.get('map_mmhg', 70)

        # Target MAP 50-60 (permissive hypotension)
        if map_mmhg < 50:
            new['crystalloid_rate_ml_min'] = 150
            new['blood_rate_ml_min'] = 100 if blood_loss_ml > 1000 else 0
            new['next_interval_s'] = 15
        elif map_mmhg < 55:
            new['crystalloid_rate_ml_min'] = 100
            new['blood_rate_ml_min'] = 50 if blood_loss_ml > 750 else 0
            new['next_interval_s'] = 20
        elif map_mmhg > 65:
            # Above target - reduce
            new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 50)
            new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
            new['next_interval_s'] = 45
        else:
            # At target
            new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 25)
            new['blood_rate_ml_min'] = 0
            new['next_interval_s'] = 30

        self.settings = new.copy()
        return new

    def _damage_control_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
        """Damage control resuscitation - prioritize blood products, limit crystalloid.

        Modern trauma approach:
        - 1:1:1 ratio (if we had plasma/platelets)
        - Limit crystalloid to avoid dilutional coagulopathy
        - Permissive hypotension until hemorrhage controlled
        """
        new = self.settings.copy()
        map_mmhg = vitals.get('map_mmhg', 70)

        # Cap crystalloid at 2L total
        max_crystalloid = max(0, 2000 - crystalloid_infused_ml)

        if map_mmhg < 50:
            # Prioritize blood
            new['blood_rate_ml_min'] = 200
            new['crystalloid_rate_ml_min'] = min(100, max_crystalloid / 10) if max_crystalloid > 0 else 0
            new['next_interval_s'] = 10
        elif map_mmhg < 60:
            new['blood_rate_ml_min'] = 150
            new['crystalloid_rate_ml_min'] = min(50, max_crystalloid / 20) if max_crystalloid > 0 else 0
            new['next_interval_s'] = 15
        elif map_mmhg > 65:
            # At target - reduce
            new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
            new['crystalloid_rate_ml_min'] = 0
            new['next_interval_s'] = 30
        else:
            new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 25)
            new['crystalloid_rate_ml_min'] = 0
            new['next_interval_s'] = 20

        self.settings = new.copy()
        return new


class PatientEventHandler(IEventHandler):
    """Handles Pulse patient events to detect death and other critical states."""

    # Events that indicate patient death or irreversible state
    DEATH_EVENTS = {
        eEvent.IrreversibleState,
        eEvent.CardiacArrest,
    }

    # Events worth logging (critical conditions)
    CRITICAL_EVENTS = {
        eEvent.IrreversibleState: "Irreversible State (Death)",
        eEvent.CardiacArrest: "Cardiac Arrest",
        eEvent.CardiovascularCollapse: "Cardiovascular Collapse",
        eEvent.HypovolemicShock: "Hypovolemic Shock",
        eEvent.CardiogenicShock: "Cardiogenic Shock",
        eEvent.CriticalBrainOxygenDeficit: "Critical Brain O2 Deficit",
        eEvent.MyocardiumOxygenDeficit: "Myocardium O2 Deficit",
        eEvent.Hypoxia: "Hypoxia",
        eEvent.LacticAcidosis: "Lactic Acidosis",
        eEvent.MetabolicAcidosis: "Metabolic Acidosis",
        eEvent.RespiratoryAcidosis: "Respiratory Acidosis",
    }

    def __init__(self):
        super().__init__(active_events_only=False)  # Get both active and inactive events
        self.is_dead = False
        self.death_time_s = None
        self.death_cause = None
        self.active_events = set()  # Currently active critical events
        self.event_log = []  # List of (time_s, event_name, active) tuples

    def handle_event(self, change: SEEventChange):
        """Called by Pulse when a patient event occurs."""
        event = change.event
        is_active = change.active
        time_s = change.sim_time.get_value(TimeUnit.s) if change.sim_time.is_valid() else 0

        # Check for death events
        if event in self.DEATH_EVENTS and is_active and not self.is_dead:
            self.is_dead = True
            self.death_time_s = time_s
            self.death_cause = self.CRITICAL_EVENTS.get(event, str(event))
            print(f"[PATIENT DEATH] {self.death_cause} at t={time_s:.1f}s")

        # Log critical events
        if event in self.CRITICAL_EVENTS:
            event_name = self.CRITICAL_EVENTS[event]
            if is_active:
                self.active_events.add(event)
                self.event_log.append((time_s, event_name, True))
                print(f"[Patient Event] {event_name} ONSET at t={time_s:.1f}s")
            else:
                self.active_events.discard(event)
                self.event_log.append((time_s, event_name, False))
                print(f"[Patient Event] {event_name} RESOLVED at t={time_s:.1f}s")

    def get_status_string(self):
        """Get a string describing current patient status."""
        if self.is_dead:
            return f"DECEASED - {self.death_cause} at {self.death_time_s:.1f}s"
        elif self.active_events:
            active_names = [self.CRITICAL_EVENTS[e] for e in self.active_events]
            return f"CRITICAL: {', '.join(active_names)}"
        else:
            return "Stable"


class SimulationRunner:
    """Runs Pulse simulation with event timeline."""

    def __init__(self, job_id, job, progress_callback, live_data_callback=None):
        self.job_id = job_id
        self.job = job
        self.progress_callback = progress_callback
        self.live_data_callback = live_data_callback
        self.pulse = None
        self.vent_settings = {}
        self.is_intubated = False
        self.vent_active = False
        self.controller = None
        self.controller_active = False
        self.fluid_controller = None
        self.fluid_controller_active = False
        self.http_fluid_controller = None
        self.fluid_settings = {
            'crystalloid_rate_ml_min': 0,
            'blood_rate_ml_min': 0,
            'crystalloid_compound': 'Saline',
            'blood_compound': 'Blood'
        }
        self.last_fluid_control_time = 0.0
        self.fluid_control_interval_s = 10.0
        self.http_controller = None
        self.http_controller_data_names = []
        self.http_duplicate_names = []
        self.http_unique_names = []
        self.http_data_start_index = 0
        self.pulse_to_vitals_map = {}
        self.init_method = "unknown"
        self.hemorrhage_flow_rates = {}
        self.hemorrhage_stop_conditions = {}  # compartment -> {vital, operator, value, maxDurationSec, startTime}
        self.cumulative_blood_loss_ml = 0.0
        self.active_infusions = {}
        self.cumulative_blood_infused_ml = 0.0
        self.cumulative_crystalloid_infused_ml = 0.0
        self.initial_blood_volume_ml = 0.0  # Stored after patient load for relative hemorrhage rates
        self.patient_weight_kg = 70.0  # Stored after patient load for weight-based dosing
        self.triggered_events = []  # Events waiting for a condition to be met
        self.fired_trigger_ids = set()  # Track which triggers have fired (one-and-done)
        self.fired_event_names = set()  # Track event names that have fired (for after_event triggers)
        self.pending_delayed_triggers = {}  # event_id -> fire_time for triggers waiting on delay
        self.cancelled = False
        self.last_live_emit_time = 0
        self.event_handler = None  # Patient event handler for death detection
        # Dynamic CSV output columns
        self.selected_vars = resolve_selected_vars(job.get('output_columns'))
        self.csv_columns = build_csv_columns(self.selected_vars)
    
    def check_cancelled(self):
        """Check if this job has been cancelled."""
        with job_lock:
            if cancel_flags.get(self.job_id):
                self.cancelled = True
                return True
        return False

    def _check_hemorrhage_conditions(self, vitals, sim_time):
        """Check if any conditional hemorrhage stop conditions are met.

        Returns list of annotation strings for any hemorrhages that were stopped.
        """
        annotations = []
        compartments_to_stop = []

        for compartment, condition in self.hemorrhage_stop_conditions.items():
            vital_name = condition["vital"]
            operator = condition["operator"]
            threshold = condition["value"]
            max_duration = condition.get("maxDurationSec", 600)
            start_time = condition.get("startTime", 0)

            # Get current vital value
            vital_value = vitals.get(vital_name)
            if vital_value is None:
                continue

            # Check if condition is met
            condition_met = False
            if operator == ">=" and vital_value >= threshold:
                condition_met = True
            elif operator == "<=" and vital_value <= threshold:
                condition_met = True
            elif operator == ">" and vital_value > threshold:
                condition_met = True
            elif operator == "<" and vital_value < threshold:
                condition_met = True
            elif operator == "==" and abs(vital_value - threshold) < 0.01:
                condition_met = True

            # Check max duration safety limit
            elapsed = sim_time - start_time
            max_duration_reached = elapsed >= max_duration

            if condition_met or max_duration_reached:
                compartments_to_stop.append(compartment)
                if condition_met:
                    reason = f"{vital_name} {operator} {threshold} (actual: {vital_value:.1f})"
                else:
                    reason = f"max duration {max_duration}s reached"
                annotations.append(f"Stop Hemorrhage {compartment} ({reason})")

        # Stop the hemorrhages
        for compartment in compartments_to_stop:
            self._stop_hemorrhage_by_condition(compartment)

        return annotations

    def _stop_hemorrhage_by_condition(self, compartment):
        """Stop a hemorrhage due to condition being met."""
        compartment_map = {
            "RightLeg": eHemorrhage_Compartment.RightLeg,
            "LeftLeg": eHemorrhage_Compartment.LeftLeg,
            "RightArm": eHemorrhage_Compartment.RightArm,
            "LeftArm": eHemorrhage_Compartment.LeftArm,
            "Aorta": eHemorrhage_Compartment.Aorta,
            "VenaCava": eHemorrhage_Compartment.VenaCava,
        }

        action = SEHemorrhage()
        action.set_compartment(compartment_map.get(compartment, eHemorrhage_Compartment.RightLeg))
        action.get_flow_rate().set_value(0, VolumePerTimeUnit.mL_Per_min)
        self.pulse.process_action(action)

        # Update tracking
        self.hemorrhage_flow_rates[compartment] = 0
        if compartment in self.hemorrhage_stop_conditions:
            del self.hemorrhage_stop_conditions[compartment]

    def _extract_patient_weight(self, custom_patient, patient_json, patient_name):
        """Extract patient weight in kg from available source data.

        Tries in order:
        1. custom_patient dict (weight_kg or weight_lb)
        2. patient_json state file (CurrentPatient.Weight)
        3. patient_json patient definition (Weight)
        4. Read from state file on disk
        5. Fallback: estimate from blood volume
        """
        weight_kg = None

        # 1. Custom patient
        if custom_patient:
            if "weight_kg" in custom_patient:
                weight_kg = custom_patient["weight_kg"]
            elif "weight_lb" in custom_patient:
                weight_kg = custom_patient["weight_lb"] * 0.453592
            print(f"[Weight] From custom_patient: {weight_kg:.1f} kg")

        # 2. Patient JSON (state file or patient definition)
        elif patient_json:
            # State file format: CurrentPatient.Weight.ScalarMass
            if "CurrentPatient" in patient_json:
                weight_data = patient_json.get("CurrentPatient", {}).get("Weight", {}).get("ScalarMass", {})
            else:
                # Patient definition format: Weight.ScalarMass
                weight_data = patient_json.get("Weight", {}).get("ScalarMass", {})

            if weight_data:
                value = weight_data.get("Value")
                unit = weight_data.get("Unit", "lb")
                if value:
                    if unit == "kg":
                        weight_kg = value
                    else:  # Default to lb
                        weight_kg = value * 0.453592
                    print(f"[Weight] From patient_json: {weight_kg:.1f} kg")

        # 3. Read from state file on disk
        elif patient_name:
            try:
                state_path = f"./states/{patient_name}" if not os.path.isabs(patient_name) else patient_name
                full_path = os.path.join(PULSE_BIN, state_path) if not os.path.isabs(state_path) else state_path
                if os.path.exists(full_path):
                    with open(full_path, 'r') as f:
                        state_data = json.load(f)
                    weight_data = state_data.get("CurrentPatient", {}).get("Weight", {}).get("ScalarMass", {})
                    if weight_data:
                        value = weight_data.get("Value")
                        unit = weight_data.get("Unit", "lb")
                        if value:
                            if unit == "kg":
                                weight_kg = value
                            else:
                                weight_kg = value * 0.453592
                            print(f"[Weight] From state file {patient_name}: {weight_kg:.1f} kg")
            except Exception as e:
                print(f"[Weight] Could not read state file: {e}")

        # 4. Fallback: estimate from blood volume
        if weight_kg is None or weight_kg < 20:
            weight_kg = self.initial_blood_volume_ml / 70.0
            if weight_kg < 20:
                weight_kg = 70.0  # Default adult weight
            print(f"[Weight] Estimated from blood volume: {weight_kg:.1f} kg")

        return weight_kg

    def _check_vital_condition(self, trigger, vitals):
        """Check if a vital-based trigger condition is met.

        Returns True if the condition is satisfied, False otherwise.
        """
        vital_name = trigger.get("vital")
        operator = trigger.get("operator")
        threshold = trigger.get("value")

        if vital_name is None or operator is None or threshold is None:
            return False

        vital_value = vitals.get(vital_name)
        if vital_value is None:
            return False

        if operator == ">=" and vital_value >= threshold:
            return True
        elif operator == "<=" and vital_value <= threshold:
            return True
        elif operator == ">" and vital_value > threshold:
            return True
        elif operator == "<" and vital_value < threshold:
            return True
        elif operator == "==" and abs(vital_value - threshold) < 0.01:
            return True

        return False

    def _check_triggered_events(self, vitals, sim_time):
        """Check if any triggered events should fire based on current vitals or fired events.

        Supports:
        - Vital-based triggers: {"vital": "spo2_pct", "operator": "<=", "value": 88}
        - Event-based triggers: {"after_event": "intubate"}
        - Delay support: {"delay_s": 30} (condition fires once, then waits delay_s before executing)

        Returns list of events that were triggered this timestep.
        """
        triggered = []

        # First, check for any pending delayed triggers that are ready to fire
        events_ready_from_delay = []
        for event_id, fire_time in list(self.pending_delayed_triggers.items()):
            if sim_time >= fire_time:
                # Find the event with this id
                for event in self.triggered_events:
                    eid = event.get("id") or id(event)
                    if eid == event_id:
                        events_ready_from_delay.append(event)
                        self.fired_trigger_ids.add(event_id)
                        break
                del self.pending_delayed_triggers[event_id]

        triggered.extend(events_ready_from_delay)

        # Now check all triggered events for new condition matches
        for event in self.triggered_events:
            event_id = event.get("id") or id(event)

            # Skip if already fired or already pending delay
            if event_id in self.fired_trigger_ids:
                continue
            if event_id in self.pending_delayed_triggers:
                continue

            trigger = event.get("trigger")
            if not trigger:
                continue

            condition_met = False

            # Check for after_event trigger (event-based)
            after_event = trigger.get("after_event")
            if after_event:
                # Check if the referenced event has fired
                if after_event in self.fired_event_names:
                    condition_met = True
            else:
                # Vital-based trigger
                condition_met = self._check_vital_condition(trigger, vitals)

            if condition_met:
                delay_s = trigger.get("delay_s", 0)
                if delay_s > 0:
                    # Schedule for later firing
                    self.pending_delayed_triggers[event_id] = sim_time + delay_s
                else:
                    # Fire immediately
                    self.fired_trigger_ids.add(event_id)
                    triggered.append(event)

        return triggered

    def run(self):
        os.chdir(PULSE_BIN)
        
        patient = self.job.get("patient", "Carol@0s.json")
        duration_s = self.job.get("duration_s", 300)
        
        step_size_s = self.job.get("step_size_s")
        if step_size_s is None:
            sample_rate_hz = self.job.get("sample_rate_hz", 50)
            step_size_s = 1.0 / sample_rate_hz
        
        timestep = max(0.02, min(0.06, step_size_s))
        
        # v6: start_intubated defaults to False - use events for intubation/ventilation
        start_intubated = self.job.get("start_intubated", False)
        # v6: vent_settings can come from job level (legacy) or from events
        vent_settings = self.job.get("vent_settings", {})

        # Separate time-based events from trigger-based events
        all_events = self.job.get("events", [])
        time_events = [e for e in all_events if "trigger" not in e]
        self.triggered_events = [e for e in all_events if "trigger" in e]
        events = sorted(time_events, key=lambda e: e.get("time", 0))
        
        pulse_config_path = os.path.join(PULSE_BIN, "PulseConfiguration.json")
        with pulse_config_lock:
            if timestep != 0.02:
                config = {"TimeStep": {"ScalarTime": {"Value": timestep, "Unit": "s"}}}
                with open(pulse_config_path, 'w') as f:
                    json.dump(config, f)
            else:
                if os.path.exists(pulse_config_path):
                    try:
                        os.remove(pulse_config_path)
                    except:
                        pass
        
        self.pulse = PulseEngine()
        self.pulse.set_log_filename("./test_results/web_ui.log")
        self.pulse.log_to_console(False)

        # Set up event handler for patient death detection
        self.event_handler = PatientEventHandler()
        self.pulse.set_event_handler(self.event_handler)
        
        # Check for HTTP controller events and pre-initialize
        http_controller_requests = []
        for event in events:
            if event.get("type") == "start_controller" and event.get("controller") == "http_controller":
                url = event.get("http_url", "http://localhost:5001")
                config = event.get("http_config", {})
                timeout = event.get("http_timeout", 10.0)

                # Build simulation context for controller identification
                simulation_context = {
                    'simulation_id': self.job_id,
                    'job_id': self.job_id
                }

                self.http_controller = HTTPController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                try:
                    http_controller_requests = self.http_controller.send_init(
                        self.job.get("patient", ""),
                        vent_settings
                    )
                    self.http_controller_data_names = [
                        req.get("name") for req in self.http_controller.data_requests
                    ]
                    print(f"[HTTPController] Initialized with {len(http_controller_requests)} data requests (job_id: {self.job_id})")
                except Exception as e:
                    raise RuntimeError(f"Failed to initialize HTTP controller at {url}: {e}")
                break
        
        # Build data requests dynamically from selected output columns
        data_requests = build_data_requests(self.selected_vars)

        # Handle HTTP controller data requests
        self.http_data_start_index = len(data_requests)

        # Build base_request_names dynamically for HTTP controller deduplication
        base_request_names = {v["pulse_name"] for v in self.selected_vars}

        # Build pulse_to_vitals_map dynamically
        self.pulse_to_vitals_map = {v["pulse_name"]: v["key"] for v in self.selected_vars}
        
        filtered_http_requests = []
        self.http_duplicate_names = []
        self.http_unique_names = []
        
        for i, req in enumerate(http_controller_requests):
            name = self.http_controller_data_names[i]
            if name in base_request_names:
                self.http_duplicate_names.append(name)
            else:
                filtered_http_requests.append(req)
                self.http_unique_names.append(name)
        
        data_requests.extend(filtered_http_requests)
        
        data_mgr = SEDataRequestManager(data_requests)
        
        custom_patient = self.job.get("custom_patient")
        patient_json = self.job.get("patient_json")
        
        if custom_patient:
            self.progress_callback(0, duration_s, None, "stabilizing", 0)
            
            pc = SEPatientConfiguration()
            p = pc.get_patient()
            
            p.set_name(custom_patient.get("name", "CustomPatient"))
            if custom_patient.get("sex") == "Female":
                p.set_sex(eSex.Female)
            else:
                p.set_sex(eSex.Male)
            
            p.get_age().set_value(custom_patient.get("age_yr", 44), TimeUnit.yr)
            p.get_height().set_value(custom_patient.get("height_in", 71), LengthUnit.inch)
            p.get_weight().set_value(custom_patient.get("weight_lb", 180), MassUnit.lb)
            p.get_heart_rate_baseline().set_value(custom_patient.get("hr_baseline", 72), FrequencyUnit.Per_min)
            p.get_systolic_arterial_pressure_baseline().set_value(custom_patient.get("sbp_baseline", 120), PressureUnit.mmHg)
            p.get_diastolic_arterial_pressure_baseline().set_value(custom_patient.get("dbp_baseline", 80), PressureUnit.mmHg)
            p.get_respiration_rate_baseline().set_value(custom_patient.get("rr_baseline", 12), FrequencyUnit.Per_min)
            
            pc.set_data_root_dir("./")
            
            if not self.pulse.initialize_engine(pc, data_mgr):
                raise RuntimeError(f"Failed to stabilize custom patient: {custom_patient.get('name')}")
            
            self.init_method = f"initialize_engine(custom: {custom_patient.get('name')})"
            self.progress_callback(0, duration_s, "Stabilization complete", "running")
        
        elif patient_json:
            # Detect if this is a pre-stabilized state file or a patient definition
            # State files have keys like "SimulationTime" or "InitialPatient"
            # Patient definitions have keys like "Name", "Sex", "Age"
            is_state_file = any(key in patient_json for key in ['SimulationTime', 'InitialPatient', 'Compartments', 'CurrentPatient'])
            
            if is_state_file:
                # Pre-stabilized state - load directly with serialize_from_file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, dir=PULSE_BIN) as f:
                    json.dump(patient_json, f)
                    temp_state_path = f.name
                
                try:
                    # Use relative path from PULSE_BIN
                    rel_path = os.path.basename(temp_state_path)
                    if not self.pulse.serialize_from_file(rel_path, data_mgr):
                        raise RuntimeError(f"Failed to load pre-stabilized state from uploaded JSON")
                    
                    self.init_method = "serialize_from_file(uploaded state)"
                finally:
                    try:
                        os.unlink(temp_state_path)
                    except:
                        pass
            else:
                # Patient definition - needs stabilization
                self.progress_callback(0, duration_s, None, "stabilizing", 0)
                
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(patient_json, f)
                    temp_patient_path = f.name
                
                try:
                    pc = SEPatientConfiguration()
                    serialize_patient_from_file(temp_patient_path, pc.get_patient())
                    pc.set_data_root_dir("./")
                    
                    if not self.pulse.initialize_engine(pc, data_mgr):
                        raise RuntimeError(f"Failed to stabilize patient from uploaded JSON")
                    
                    self.init_method = "initialize_engine(uploaded patient definition)"
                    self.progress_callback(0, duration_s, "Stabilization complete", "running")
                finally:
                    try:
                        os.unlink(temp_patient_path)
                    except:
                        pass
        
        else:
            patient_path = f"./states/{patient}" if not os.path.isabs(patient) else patient
            
            if not self.pulse.serialize_from_file(patient_path, data_mgr):
                raise RuntimeError(f"Failed to load patient: {patient}")
            
            self.init_method = f"serialize_from_file({patient})"
        
        self.vent_settings = vent_settings
        self._pending_bolus_stop = None

        # Capture initial blood volume for relative hemorrhage rate calculations
        # Advance time slightly to ensure engine has computed state values
        self.pulse.advance_time_s(0.1)
        initial_results = self.pulse.pull_data()
        # Use dynamic vitals dict to get blood volume by key (index depends on selected columns)
        initial_vitals = build_vitals_dict(initial_results, self.selected_vars)
        self.initial_blood_volume_ml = initial_vitals.get("blood_volume_ml", 5000.0)
        if self.initial_blood_volume_ml <= 0:
            self.initial_blood_volume_ml = 5000.0
        print(f"[Init] Patient initial blood volume: {self.initial_blood_volume_ml:.0f} mL")

        # Extract patient weight from source data for weight-based drug dosing
        self.patient_weight_kg = self._extract_patient_weight(custom_patient, patient_json, patient)
        print(f"[Init] Patient weight: {self.patient_weight_kg:.1f} kg")

        if start_intubated:
            self.do_intubate()
            self.do_start_vent()
        
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(self.csv_columns)
        
        sim_time = 0.0
        self._current_sim_time = 0.0  # Track for conditional hemorrhage
        last_control_time = 0.0
        control_interval_s = 1.0
        event_idx = 0
        last_event_name = None

        while sim_time < duration_s:
            # Check for cancellation
            if self.check_cancelled():
                break

            self._current_sim_time = sim_time  # Update for process_event to use

            current_events = []
            while event_idx < len(events) and events[event_idx].get("time", 0) <= sim_time:
                event = events[event_idx]
                last_event_name = self.process_event(event)
                if last_event_name:
                    current_events.append(last_event_name)
                    # Track event type for after_event triggers
                    etype = event.get("type")
                    if etype:
                        self.fired_event_names.add(etype)
                event_idx += 1
            
            event_annotation = "; ".join(current_events) if current_events else ""
            
            self.pulse.advance_time_s(timestep)
            sim_time += timestep
            
            if hasattr(self, '_pending_bolus_stop') and self._pending_bolus_stop:
                try:
                    stop_infusion = SESubstanceInfusion()
                    stop_infusion.set_substance(self._pending_bolus_stop)
                    stop_infusion.get_rate().set_value(0, VolumePerTimeUnit.mL_Per_min)
                    stop_infusion.get_concentration().set_value(0.001, MassPerVolumeUnit.from_string("g/L"))
                    self.pulse.process_action(stop_infusion)
                except:
                    pass
                self._pending_bolus_stop = None
            
            results = self.pulse.pull_data()
            vitals = self.get_vitals_dict(results)
            
            if not hasattr(self, '_patient_dead'):
                self._patient_dead = False
                self._death_logged = False
                self._sim_stalled = False
                self._prev_hr = None
                self._stall_count = 0

            # Check for patient death via event handler (Pulse's official death events)
            if self.event_handler and self.event_handler.is_dead and not self._death_logged:
                self._patient_dead = True
                self._death_logged = True
                death_msg = f"PATIENT_DEATH ({self.event_handler.death_cause})"
                if event_annotation:
                    event_annotation += "; " + death_msg
                else:
                    event_annotation = death_msg

            # Fallback: also check vitals for death (in case events don't fire)
            if not self._patient_dead:
                hr = vitals.get("hr_bpm", 999)
                map_val = vitals.get("map_mmhg", 999)
                co = vitals.get("co_lpm", 999)
                if hr < 1 or map_val < 5 or co < 0.1:
                    self._patient_dead = True
                    self._death_logged = True
                    if event_annotation:
                        event_annotation += "; PATIENT_DEATH (vitals)"
                    else:
                        event_annotation = "PATIENT_DEATH (vitals)"

            if not self._sim_stalled and self._prev_hr is not None:
                hr = vitals.get("hr_bpm", 0)
                if abs(hr - self._prev_hr) < 0.0001 and hr > 0:
                    self._stall_count += 1
                    if self._stall_count >= 50:
                        self._sim_stalled = True
                        if event_annotation:
                            event_annotation += "; ENGINE_STALLED"
                        else:
                            event_annotation = "ENGINE_STALLED"
                        self.progress_callback(sim_time, duration_s, "ENGINE STALLED (IrreversibleState)", "running")
                else:
                    self._stall_count = 0
            self._prev_hr = vitals.get("hr_bpm", 0)

            # Check conditional hemorrhage stops
            condition_stops = self._check_hemorrhage_conditions(vitals, sim_time)
            for stop_annotation in condition_stops:
                if event_annotation:
                    event_annotation += "; " + stop_annotation
                else:
                    event_annotation = stop_annotation

            # Check triggered events (condition-based interventions)
            triggered_now = self._check_triggered_events(vitals, sim_time)
            for triggered_event in triggered_now:
                trigger = triggered_event.get("trigger", {})
                # Build trigger description based on trigger type
                after_event = trigger.get("after_event")
                delay_s = trigger.get("delay_s", 0)
                if after_event:
                    trigger_desc = f"after:{after_event}"
                    if delay_s > 0:
                        trigger_desc += f"+{delay_s}s"
                else:
                    trigger_desc = f"{trigger.get('vital')} {trigger.get('operator')} {trigger.get('value')}"
                    if delay_s > 0:
                        trigger_desc += f"+{delay_s}s delay"
                event_name = self.process_event(triggered_event)
                if event_name:
                    triggered_annotation = f"TRIGGERED[{trigger_desc}]: {event_name}"
                    if event_annotation:
                        event_annotation += "; " + triggered_annotation
                    else:
                        event_annotation = triggered_annotation
                    # Track event type for after_event triggers (enables chaining)
                    etype = triggered_event.get("type")
                    if etype:
                        self.fired_event_names.add(etype)

            # Controller
            controller_cmd = ""
            if self.controller_active and self.controller and (sim_time - last_control_time) >= control_interval_s:
                if self.http_controller and self.controller == self.http_controller:
                    # Always include standard vitals for HTTP controller
                    http_data = {
                        "sim_time_s": sim_time,
                        "spo2_pct": vitals.get("spo2_pct", 0),
                        "paco2_mmhg": vitals.get("paco2_mmhg", 0),
                        "ph": vitals.get("ph", 0),
                        "hr_bpm": vitals.get("hr_bpm", 0),
                        "map_mmhg": vitals.get("map_mmhg", 0),
                        "sbp_mmhg": vitals.get("sbp_mmhg", 0),
                        "dbp_mmhg": vitals.get("dbp_mmhg", 0),
                        "etco2_mmhg": vitals.get("etco2_mmhg", 0),
                        "rr_patient": vitals.get("rr_patient", 0),
                        "tv_ml": vitals.get("tv_ml", 0),
                        "lactate_mmol_l": vitals.get("lactate_mmol_l", 0),
                    }

                    # Add any custom data requests from the controller
                    for var_name in self.http_duplicate_names:
                        vitals_key = self.pulse_to_vitals_map.get(var_name)
                        if vitals_key and vitals_key in vitals:
                            value = vitals[vitals_key]
                            if var_name == "OxygenSaturation":
                                value = value / 100.0
                            http_data[var_name] = value

                    for i, var_name in enumerate(self.http_unique_names):
                        data_idx = self.http_data_start_index + i
                        if data_idx < len(results):
                            http_data[var_name] = results[data_idx]

                    response = self.controller.step(http_data, self.vent_settings)
                else:
                    response = self.controller.step(vitals, self.vent_settings)
                    
                if response:
                    new_settings = self.vent_settings.copy()
                    changes = []
                    for key in ['mode', 'vt_ml', 'rr', 'fio2', 'peep_cmh2o', 'pinsp_cmh2o', 'itime_s']:
                        if key in response:
                            old_val = self.vent_settings.get(key)
                            new_val = response[key]
                            if old_val != new_val:
                                if key == 'fio2':
                                    changes.append(f"FiO2:{old_val:.0%}->{new_val:.0%}")
                                elif key == 'mode':
                                    changes.append(f"Mode:{new_val}")
                                else:
                                    key_short = key.replace('_cmh2o', '').replace('_ml', '').replace('_s', '')
                                    changes.append(f"{key_short}:{old_val}->{new_val}")
                            new_settings[key] = new_val
                    if changes:
                        controller_cmd = "; ".join(changes)
                    if self.vent_active:
                        self.apply_vent_settings(new_settings)
                    self.vent_settings = new_settings
                    if 'next_interval_s' in response:
                        control_interval_s = max(0.1, response['next_interval_s'])
                last_control_time = sim_time

            # Fluid Controller
            fluid_cmd = ""
            if self.fluid_controller_active and self.fluid_controller and (sim_time - self.last_fluid_control_time) >= self.fluid_control_interval_s:
                # Build vitals dict for fluid controller
                if hasattr(self, 'http_fluid_controller') and self.fluid_controller == self.http_fluid_controller:
                    # HTTP fluid controller needs sim_time and standard vitals
                    fluid_vitals = {
                        "sim_time_s": sim_time,
                        "hr_bpm": vitals.get("hr_bpm", 0),
                        "map_mmhg": vitals.get("map_mmhg", 0),
                        "sbp_mmhg": vitals.get("sbp_mmhg", 0),
                        "dbp_mmhg": vitals.get("dbp_mmhg", 0),
                        "spo2_pct": vitals.get("spo2_pct", 0),
                        "lactate_mmol_l": vitals.get("lactate_mmol_l", 0),
                    }
                else:
                    fluid_vitals = vitals

                fluid_response = self.fluid_controller.step(
                    fluid_vitals,
                    self.fluid_settings,
                    blood_loss_ml=self.cumulative_blood_loss_ml,
                    blood_infused_ml=self.cumulative_blood_infused_ml,
                    crystalloid_infused_ml=self.cumulative_crystalloid_infused_ml
                )

                if fluid_response:
                    changes = []
                    # Apply crystalloid rate changes
                    new_crystalloid_rate = fluid_response.get('crystalloid_rate_ml_min', 0)
                    old_crystalloid_rate = self.fluid_settings.get('crystalloid_rate_ml_min', 0)
                    if new_crystalloid_rate != old_crystalloid_rate:
                        changes.append(f"Cryst:{old_crystalloid_rate}->{new_crystalloid_rate}mL/min")
                        self._apply_fluid_infusion('crystalloid', new_crystalloid_rate)

                    # Apply blood rate changes
                    new_blood_rate = fluid_response.get('blood_rate_ml_min', 0)
                    old_blood_rate = self.fluid_settings.get('blood_rate_ml_min', 0)
                    if new_blood_rate != old_blood_rate:
                        changes.append(f"Blood:{old_blood_rate}->{new_blood_rate}mL/min")
                        self._apply_fluid_infusion('blood', new_blood_rate)

                    if changes:
                        fluid_cmd = "; ".join(changes)

                    self.fluid_settings.update(fluid_response)
                    if 'next_interval_s' in fluid_response:
                        self.fluid_control_interval_s = max(1.0, fluid_response['next_interval_s'])

                self.last_fluid_control_time = sim_time

            # Blood loss tracking
            total_flow_rate = sum(self.hemorrhage_flow_rates.values())
            blood_loss_this_step = total_flow_rate * (timestep / 60.0)
            self.cumulative_blood_loss_ml += blood_loss_this_step
            
            # Infusion tracking
            infusions_to_remove = []
            for compound, info in self.active_infusions.items():
                if info['remaining_ml'] > 0:
                    volume_this_step = info['rate'] * (timestep / 60.0)
                    actual_volume = min(volume_this_step, info['remaining_ml'])
                    info['remaining_ml'] -= actual_volume
                    
                    if info['is_blood']:
                        self.cumulative_blood_infused_ml += actual_volume
                    else:
                        self.cumulative_crystalloid_infused_ml += actual_volume
                    
                    if info['remaining_ml'] <= 0:
                        infusions_to_remove.append(compound)
            
            for compound in infusions_to_remove:
                del self.active_infusions[compound]
            
            writer.writerow(self.get_csv_row(vitals, event_annotation, controller_cmd, fluid_cmd))
            self.progress_callback(sim_time, duration_s, last_event_name, "running")

            # Emit live data for real-time plotting (throttled to ~1Hz)
            if self.live_data_callback and (sim_time - self.last_live_emit_time) >= 1.0:
                self.live_data_callback(sim_time, vitals)
                self.last_live_emit_time = sim_time
        
        if self.http_controller:
            self.http_controller.shutdown()

        if self.http_fluid_controller:
            self.http_fluid_controller.shutdown()

        # Add summary note at end of CSV if patient died
        if self._patient_dead and self.event_handler:
            writer.writerow([])  # Empty row
            writer.writerow(["# SIMULATION SUMMARY"])
            writer.writerow([f"# Patient Status: {self.event_handler.get_status_string()}"])
            if self.event_handler.death_time_s:
                writer.writerow([f"# Time of Death: {self.event_handler.death_time_s:.1f}s"])
            if self.event_handler.event_log:
                writer.writerow(["# Critical Events:"])
                for time_s, event_name, active in self.event_handler.event_log:
                    status = "ONSET" if active else "RESOLVED"
                    writer.writerow([f"#   {time_s:.1f}s - {event_name} ({status})"])

        return csv_buffer.getvalue()
    
    def process_event(self, event):
        etype = event.get("type")
        
        if etype == "intubate":
            intubation_type = event.get("intubationType", "Tracheal")
            self.do_intubate(intubation_type)
            return f"Intubated ({intubation_type})"
        
        elif etype == "start_vent":
            # v6: vent_settings can now be embedded in the event
            if "vent_settings" in event:
                self.vent_settings = event["vent_settings"]
            self.do_start_vent()
            return "Vent started"

        elif etype == "change_vent":
            # v6: Change ventilator settings mid-simulation
            if "vent_settings" in event:
                self.vent_settings = event["vent_settings"]
                self.apply_vent_settings(self.vent_settings)
                return "Vent settings changed"
            return "Change vent (no settings)"
        
        elif etype == "pathology":
            ptype = event.get("pathologyType", "ARDS")
            severity = event.get("severity", 0.5)
            
            if ptype == "ARDS":
                action = SEAcuteRespiratoryDistressSyndromeExacerbation()
                action.get_severity(eLungCompartment.LeftLung).set_value(severity)
                action.get_severity(eLungCompartment.RightLung).set_value(severity)
                self.pulse.process_action(action)
                return f"ARDS ({severity})"
            
            elif ptype == "AirwayObstruction":
                action = SEAirwayObstruction()
                action.get_severity().set_value(severity)
                self.pulse.process_action(action)
                return f"Airway Obstruction ({severity})"
            
            elif ptype == "AcuteStress":
                action = SEAcuteStress()
                action.get_severity().set_value(severity)
                self.pulse.process_action(action)
                return f"Acute Stress ({severity})"
            
            elif ptype == "Hemorrhage":
                compartment_str = event.get("compartment", "RightLeg")
                is_auto_stop = event.get("isAutoStop", False)
                is_condition_stop = event.get("isConditionStop", False)
                flow_rate = event.get("flowRate", 100)
                flow_rate_mode = event.get("flowRateMode", "absolute")

                # Convert percentage of blood volume to absolute mL/min if needed
                if flow_rate_mode == "percent_bv" and self.initial_blood_volume_ml > 0:
                    absolute_rate = (flow_rate / 100.0) * self.initial_blood_volume_ml
                    print(f"[Hemorrhage] Converting {flow_rate}% BV/min to {absolute_rate:.1f} mL/min (initial BV: {self.initial_blood_volume_ml:.0f} mL)")
                    flow_rate = absolute_rate

                action = SEHemorrhage()

                compartment_map = {
                    "RightLeg": eHemorrhage_Compartment.RightLeg,
                    "LeftLeg": eHemorrhage_Compartment.LeftLeg,
                    "RightArm": eHemorrhage_Compartment.RightArm,
                    "LeftArm": eHemorrhage_Compartment.LeftArm,
                    "Aorta": eHemorrhage_Compartment.Aorta,
                    "VenaCava": eHemorrhage_Compartment.VenaCava,
                }
                action.set_compartment(compartment_map.get(compartment_str, eHemorrhage_Compartment.RightLeg))

                action.get_flow_rate().set_value(flow_rate, VolumePerTimeUnit.mL_Per_min)
                self.pulse.process_action(action)

                self.hemorrhage_flow_rates[compartment_str] = flow_rate

                # Register stop condition if this is a conditional hemorrhage
                stop_condition = event.get("stopCondition")
                if stop_condition and flow_rate > 0:
                    self.hemorrhage_stop_conditions[compartment_str] = {
                        "vital": stop_condition.get("vital", "hr_bpm"),
                        "operator": stop_condition.get("operator", ">="),
                        "value": stop_condition.get("value", 120),
                        "maxDurationSec": event.get("maxDurationSec", 600),
                        "startTime": self._current_sim_time
                    }

                if flow_rate == 0 or is_auto_stop:
                    # Clear the stop condition when hemorrhage stops
                    if compartment_str in self.hemorrhage_stop_conditions:
                        del self.hemorrhage_stop_conditions[compartment_str]
                    return f"Stop Hemorrhage {compartment_str}"
                elif is_condition_stop:
                    # Clear the stop condition when hemorrhage stops via condition
                    if compartment_str in self.hemorrhage_stop_conditions:
                        del self.hemorrhage_stop_conditions[compartment_str]
                    condition_info = event.get("conditionMet", "condition")
                    return f"Stop Hemorrhage {compartment_str} ({condition_info})"
                else:
                    total_vol = event.get("totalVolume", 0)
                    stop_condition = event.get("stopCondition")
                    if stop_condition:
                        vital = stop_condition.get("vital", "hr_bpm")
                        op = stop_condition.get("operator", ">=")
                        val = stop_condition.get("value", 120)
                        return f"Hemorrhage {compartment_str} ({flow_rate} mL/min until {vital} {op} {val})"
                    else:
                        return f"Hemorrhage {compartment_str} ({total_vol} mL @ {flow_rate} mL/min)"
            
            return f"{ptype} ({severity})"
        
        elif etype == "start_controller":
            controller_name = event.get("controller", "default_controller")

            if controller_name == "http_controller":
                url = event.get("http_url", "http://localhost:5001")

                # Initialize HTTP controller on-demand if not already created
                if not self.http_controller:
                    try:
                        config = event.get("config", {})
                        timeout = event.get("timeout", 5.0)
                        simulation_context = {
                            'simulation_id': self.job_id,
                            'job_id': self.job_id
                        }
                        self.http_controller = HTTPController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                        http_controller_requests = self.http_controller.send_init(
                            self.job.get("patient", ""),
                            self.vent_settings
                        )
                        self.http_controller_data_names = [
                            req.get("name") for req in self.http_controller.data_requests
                        ]
                        print(f"[HTTPController] Initialized via event with {len(http_controller_requests)} data requests")
                    except Exception as e:
                        print(f"[HTTPController] Failed to initialize: {e}")
                        return f"HTTP Controller FAILED: {e}"

                self.controller = self.http_controller
                self.controller_active = True
                return f"HTTP Controller ({url})"
            else:
                self.controller = BuiltinController(controller_name)
                self.controller.send_init(self.job.get("patient", ""), self.vent_settings)
                self.controller_active = True
                return f"Controller: {controller_name}"

        elif etype == "start_fluid_controller":
            controller_name = event.get("controller", "default_fluid_controller")

            if controller_name == "http_fluid_controller":
                url = event.get("http_url", "http://localhost:5001/fluid")

                # Initialize HTTP fluid controller on-demand
                try:
                    config = event.get("config", {})
                    timeout = event.get("timeout", 5.0)
                    simulation_context = {
                        'simulation_id': self.job_id,
                        'job_id': self.job_id
                    }
                    self.http_fluid_controller = HTTPFluidController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                    self.http_fluid_controller.send_init(
                        self.job.get("patient", ""),
                        self.fluid_settings
                    )
                    self.fluid_controller = self.http_fluid_controller
                    self.fluid_controller_active = True
                    self.last_fluid_control_time = self._current_sim_time
                    print(f"[HTTPFluidController] Initialized via event")
                    return f"HTTP Fluid Controller ({url})"
                except Exception as e:
                    print(f"[HTTPFluidController] Failed to initialize: {e}")
                    return f"HTTP Fluid Controller FAILED: {e}"
            else:
                self.fluid_controller = BuiltinFluidController(controller_name)
                self.fluid_controller.send_init(self.job.get("patient", ""), self.fluid_settings)
                self.fluid_controller_active = True
                self.last_fluid_control_time = self._current_sim_time
                return f"Fluid Controller: {controller_name}"

        elif etype == "stop_fluid_controller":
            if self.fluid_controller_active:
                self.fluid_controller_active = False
                # Shutdown HTTP fluid controller if active
                if self.http_fluid_controller:
                    self.http_fluid_controller.shutdown()
                    self.http_fluid_controller = None
                # Stop any active infusions from the controller
                self._apply_fluid_infusion('crystalloid', 0)
                self._apply_fluid_infusion('blood', 0)
                self.fluid_settings['crystalloid_rate_ml_min'] = 0
                self.fluid_settings['blood_rate_ml_min'] = 0
                return "Fluid Controller stopped"
            return "Fluid Controller (already inactive)"

        elif etype == "bolus":
            drug = event.get("drug", "Rocuronium")
            route = event.get("route", "Intravenous")
            dose_mode = event.get("dose_mode", "fixed")
            concentration = event.get("concentration", 10)
            conc_unit = event.get("concentration_unit", "mg/mL")

            if isinstance(concentration, (list, tuple)):
                concentration = concentration[0] if concentration else 10
            concentration = float(concentration)

            # Calculate dose based on mode
            if dose_mode == "weight":
                # Weight-based dosing
                dose_per_kg = event.get("dose_per_kg", 0.5)
                dose_per_kg_unit = event.get("dose_per_kg_unit", "mg/kg")

                if isinstance(dose_per_kg, (list, tuple)):
                    dose_per_kg = dose_per_kg[0] if dose_per_kg else 0.5
                dose_per_kg = float(dose_per_kg)

                # Calculate total mass in mg based on patient weight
                if dose_per_kg_unit in ("ug/kg", "mcg/kg"):
                    total_mass_mg = (dose_per_kg / 1000.0) * self.patient_weight_kg
                else:  # mg/kg
                    total_mass_mg = dose_per_kg * self.patient_weight_kg

                # Calculate volume from total mass and concentration
                # concentration is in conc_unit, need to convert to mg/mL for volume calculation
                if conc_unit == "mg/mL":
                    conc_mg_per_mL = concentration
                elif conc_unit == "ug/mL":
                    conc_mg_per_mL = concentration / 1000.0
                elif conc_unit == "g/L":
                    conc_mg_per_mL = concentration  # g/L = mg/mL
                else:
                    conc_mg_per_mL = concentration

                dose_mL = total_mass_mg / conc_mg_per_mL if conc_mg_per_mL > 0 else 1.0
                dose_desc = f"{dose_per_kg}{dose_per_kg_unit} ({self.patient_weight_kg:.1f}kg)"
                print(f"[Bolus] Weight-based: {drug} {dose_per_kg}{dose_per_kg_unit} x {self.patient_weight_kg:.1f}kg = {total_mass_mg:.2f}mg ({dose_mL:.2f}mL)")
            else:
                # Fixed dose
                dose_mL = event.get("dose_mL", 50)
                if isinstance(dose_mL, (list, tuple)):
                    dose_mL = dose_mL[0] if dose_mL else 50
                dose_mL = float(dose_mL)

                if conc_unit == "mg/mL":
                    total_mass_mg = dose_mL * concentration
                elif conc_unit == "ug/mL":
                    total_mass_mg = dose_mL * concentration / 1000.0
                elif conc_unit == "g/L":
                    total_mass_mg = dose_mL * concentration
                else:
                    total_mass_mg = dose_mL * concentration
                dose_desc = f"{total_mass_mg:.1f}mg"

            # Convert concentration to g/L for Pulse
            if conc_unit == "mg/mL":
                conc_g_L = concentration
            elif conc_unit == "ug/mL":
                conc_g_L = concentration / 1000.0
            elif conc_unit == "g/L":
                conc_g_L = concentration
            else:
                conc_g_L = concentration

            try:
                infusion = SESubstanceInfusion()
                infusion.set_comment(f"Bolus delivery of {drug}")
                infusion.set_substance(drug)
                infusion.get_rate().set_value(dose_mL * 60.0, VolumePerTimeUnit.mL_Per_min)
                infusion.get_concentration().set_value(conc_g_L, MassPerVolumeUnit.from_string("g/L"))

                self.pulse.process_action(infusion)
                self._pending_bolus_stop = drug

                return f"Bolus: {drug} {dose_desc}"
            except Exception as e:
                print(f"Bolus via infusion failed: {e}")
                return f"Bolus FAILED: {drug}"
        
        elif etype == "infusion":
            drug = event.get("drug", "Propofol")
            rate = event.get("rate_mL_per_min", 100)
            concentration = event.get("concentration", 10)
            conc_unit = event.get("concentration_unit", "mg/mL")
            
            if isinstance(rate, (list, tuple)):
                rate = rate[0] if rate else 100
            if isinstance(concentration, (list, tuple)):
                concentration = concentration[0] if concentration else 10
            rate = float(rate)
            concentration = float(concentration)
            
            if conc_unit == "mg/mL":
                conc_g_L = concentration
            elif conc_unit == "ug/mL":
                conc_g_L = concentration / 1000.0
            elif conc_unit == "g/L":
                conc_g_L = concentration
            else:
                conc_g_L = concentration
            
            try:
                infusion = SESubstanceInfusion()
                infusion.set_substance(drug)
                infusion.get_rate().set_value(rate, VolumePerTimeUnit.mL_Per_min)
                infusion.get_concentration().set_value(conc_g_L, MassPerVolumeUnit.from_string("g/L"))
                self.pulse.process_action(infusion)
                return f"Infusion: {drug} @ {rate}mL/min"
            except Exception as e:
                print(f"Infusion failed: {e}")
                return f"Infusion FAILED: {drug}"
        
        elif etype == "compound_infusion":
            compound = event.get("compound", "Saline")
            rate = event.get("rate_mL_per_min", 100)
            bag_volume = event.get("bag_volume_mL", 1000)
            
            if isinstance(rate, (list, tuple)):
                rate = rate[0] if rate else 100
            rate = float(rate)
            
            try:
                infusion = SESubstanceCompoundInfusion()
                infusion.set_compound(compound)
                infusion.get_rate().set_value(rate, VolumePerTimeUnit.mL_Per_min)
                infusion.get_bag_volume().set_value(bag_volume, VolumeUnit.mL)
                self.pulse.process_action(infusion)

                is_blood = compound in ["WholeBlood", "PackedRBC", "Blood"]
                self.active_infusions[compound] = {
                    'rate': rate,
                    'remaining_ml': bag_volume,
                    'is_blood': is_blood
                }

                return f"Compound: {compound} {bag_volume}mL @ {rate}mL/min"
            except Exception as e:
                print(f"Compound infusion failed: {e}")
                return f"Compound FAILED: {compound}"
        
        elif etype == "exercise":
            intensity = event.get("intensity", 0.5)
            try:
                action = SEExercise()
                action.get_intensity().set_value(intensity)
                self.pulse.process_action(action)
                return f"Exercise ({intensity})"
            except Exception as e:
                print(f"Exercise failed: {e}")
                return f"Exercise FAILED"
        
        return None
    
    def do_intubate(self, intubation_type='Tracheal'):
        intubation = SEIntubation()
        # Map string type to enum
        type_map = {
            'Tracheal': eIntubationType.Tracheal,
            'RightMainstem': eIntubationType.RightMainstem,
            'LeftMainstem': eIntubationType.LeftMainstem,
            'Esophageal': eIntubationType.Esophageal,
            'Oropharyngeal': eIntubationType.Oropharyngeal,
            'Nasopharyngeal': eIntubationType.Nasopharyngeal,
            'Off': eIntubationType.Off
        }
        pulse_type = type_map.get(intubation_type, eIntubationType.Tracheal)
        intubation.set_type(pulse_type)
        self.pulse.process_action(intubation)
        self.is_intubated = (intubation_type != 'Off')
    
    def do_start_vent(self):
        mode = self.vent_settings.get("mode", "VC-AC")
        
        if mode.startswith("PC"):
            vent = SEMechanicalVentilatorPressureControl()
            vent.set_connection(eSwitch.On)
            if "CMV" in mode:
                vent.set_mode(eMechanicalVentilator_PressureControlMode.ContinuousMandatoryVentilation)
            elif "IMV" in mode:
                vent.set_mode(eMechanicalVentilator_PressureControlMode.IntermittentMandatoryVentilation)
            else:
                vent.set_mode(eMechanicalVentilator_PressureControlMode.AssistedControl)
            vent.get_fraction_inspired_oxygen().set_value(self.vent_settings.get("fio2", 0.4))
            vent.get_inspiratory_period().set_value(self.vent_settings.get("itime_s", 1.0), TimeUnit.s)
            vent.get_inspiratory_pressure().set_value(self.vent_settings.get("pinsp_cmh2o", 15), PressureUnit.cmH2O)
            vent.get_positive_end_expired_pressure().set_value(self.vent_settings.get("peep_cmh2o", 5), PressureUnit.cmH2O)
            vent.get_respiration_rate().set_value(self.vent_settings.get("rr", 14), FrequencyUnit.Per_min)
            vent.get_slope().set_value(self.vent_settings.get("rise_time_s", 0.2), TimeUnit.s)
        else:
            vent = SEMechanicalVentilatorVolumeControl()
            vent.set_connection(eSwitch.On)
            if "CMV" in mode:
                vent.set_mode(eMechanicalVentilator_VolumeControlMode.ContinuousMandatoryVentilation)
            elif "IMV" in mode:
                vent.set_mode(eMechanicalVentilator_VolumeControlMode.IntermittentMandatoryVentilation)
            else:
                vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
            vent.get_fraction_inspired_oxygen().set_value(self.vent_settings.get("fio2", 0.4))
            vent.get_inspiratory_period().set_value(self.vent_settings.get("itime_s", 1.0), TimeUnit.s)
            vent.get_tidal_volume().set_value(self.vent_settings.get("vt_ml", 420), VolumeUnit.mL)
            vent.get_positive_end_expired_pressure().set_value(self.vent_settings.get("peep_cmh2o", 5), PressureUnit.cmH2O)
            vent.get_respiration_rate().set_value(self.vent_settings.get("rr", 14), FrequencyUnit.Per_min)
            vent.get_flow().set_value(self.vent_settings.get("flow_lpm", 50), VolumePerTimeUnit.L_Per_min)
        
        self.pulse.process_action(vent)
        self.vent_active = True
    
    def apply_vent_settings(self, settings):
        mode = settings.get("mode", "VC-AC")
        
        if mode.startswith("PC"):
            vent = SEMechanicalVentilatorPressureControl()
            vent.set_connection(eSwitch.On)
            if "CMV" in mode:
                vent.set_mode(eMechanicalVentilator_PressureControlMode.ContinuousMandatoryVentilation)
            elif "IMV" in mode:
                vent.set_mode(eMechanicalVentilator_PressureControlMode.IntermittentMandatoryVentilation)
            else:
                vent.set_mode(eMechanicalVentilator_PressureControlMode.AssistedControl)
            vent.get_fraction_inspired_oxygen().set_value(settings.get("fio2", 0.4))
            vent.get_inspiratory_period().set_value(settings.get("itime_s", 1.0), TimeUnit.s)
            vent.get_inspiratory_pressure().set_value(settings.get("pinsp_cmh2o", 15), PressureUnit.cmH2O)
            vent.get_positive_end_expired_pressure().set_value(settings.get("peep_cmh2o", 5), PressureUnit.cmH2O)
            vent.get_respiration_rate().set_value(settings.get("rr", 14), FrequencyUnit.Per_min)
            vent.get_slope().set_value(settings.get("rise_time_s", 0.2), TimeUnit.s)
        else:
            vent = SEMechanicalVentilatorVolumeControl()
            vent.set_connection(eSwitch.On)
            if "CMV" in mode:
                vent.set_mode(eMechanicalVentilator_VolumeControlMode.ContinuousMandatoryVentilation)
            elif "IMV" in mode:
                vent.set_mode(eMechanicalVentilator_VolumeControlMode.IntermittentMandatoryVentilation)
            else:
                vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
            vent.get_fraction_inspired_oxygen().set_value(settings.get("fio2", 0.4))
            vent.get_inspiratory_period().set_value(settings.get("itime_s", 1.0), TimeUnit.s)
            vent.get_tidal_volume().set_value(settings.get("vt_ml", 420), VolumeUnit.mL)
            vent.get_positive_end_expired_pressure().set_value(settings.get("peep_cmh2o", 5), PressureUnit.cmH2O)
            vent.get_respiration_rate().set_value(settings.get("rr", 14), FrequencyUnit.Per_min)
            vent.get_flow().set_value(settings.get("flow_lpm", 50), VolumePerTimeUnit.L_Per_min)
        
        self.pulse.process_action(vent)

    def _apply_fluid_infusion(self, fluid_type, rate_ml_min):
        """Apply or update a fluid infusion via Pulse.

        Args:
            fluid_type: 'crystalloid' or 'blood'
            rate_ml_min: Rate in mL/min (0 to stop)
        """
        if fluid_type == 'crystalloid':
            compound = self.fluid_settings.get('crystalloid_compound', 'Saline')
        else:
            compound = self.fluid_settings.get('blood_compound', 'Blood')

        try:
            infusion = SESubstanceCompoundInfusion()
            infusion.set_compound(compound)
            infusion.get_rate().set_value(rate_ml_min, VolumePerTimeUnit.mL_Per_min)
            # Large bag volume - controller manages rate, not volume
            infusion.get_bag_volume().set_value(10000, VolumeUnit.mL)
            self.pulse.process_action(infusion)

            # Track in active_infusions for cumulative tracking
            is_blood = fluid_type == 'blood'
            if rate_ml_min > 0:
                self.active_infusions[compound] = {
                    'rate': rate_ml_min,
                    'remaining_ml': 10000,  # Large bag, rate-limited
                    'is_blood': is_blood
                }
            elif compound in self.active_infusions:
                del self.active_infusions[compound]

        except Exception as e:
            print(f"Fluid infusion error ({fluid_type}): {e}")

    def get_vitals_dict(self, results):
        return build_vitals_dict(results, self.selected_vars)

    def get_csv_row(self, vitals, event="", controller_cmd="", fluid_cmd=""):
        s = self.vent_settings
        f = self.fluid_settings
        row = [vitals["t"]]
        row.extend([vitals.get(v["key"], "") for v in self.selected_vars])
        row.extend([
            s.get("mode", ""), s.get("vt_ml", ""), s.get("rr", ""), s.get("fio2", ""),
            s.get("peep_cmh2o", ""), s.get("pinsp_cmh2o", ""), s.get("itime_s", ""),
            int(self.is_intubated), int(self.vent_active), int(self.controller_active),
            int(self.fluid_controller_active),
            round(self.cumulative_blood_loss_ml, 1),
            round(self.cumulative_blood_infused_ml, 1),
            round(self.cumulative_crystalloid_infused_ml, 1),
            f.get("crystalloid_rate_ml_min", 0), f.get("blood_rate_ml_min", 0),
            event, controller_cmd, fluid_cmd
        ])
        return row


# === API ROUTES ===

def get_patients():
    states_dir = os.path.join(PULSE_BIN, "states")
    patients = []
    if os.path.exists(states_dir):
        for f in os.listdir(states_dir):
            if f.endswith("@0s.json"):
                patients.append(f)
    return sorted(patients)


def run_job_thread(job_id, job):
    replicates = job.get('replicates', 1)
    total_replicates = max(1, replicates)

    def progress_callback(sim_time, duration, last_event, status="running", stabilize_time=None, replicate=1):
        with job_lock:
            jobs[job_id]['status'] = status
            jobs[job_id]['sim_time'] = sim_time
            jobs[job_id]['duration'] = duration
            jobs[job_id]['last_event'] = last_event
            jobs[job_id]['current_replicate'] = replicate
            jobs[job_id]['total_replicates'] = total_replicates
            if stabilize_time is not None:
                jobs[job_id]['stabilize_time'] = stabilize_time

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_paths = []
        total_samples = 0
        cancelled = False

        for rep in range(1, total_replicates + 1):
            # Check for cancellation between replicates
            if job_id in cancel_flags:
                cancelled = True
                break

            # Create a progress callback that includes the replicate number
            def rep_progress_callback(sim_time, duration, last_event, status="running", stabilize_time=None, r=rep):
                progress_callback(sim_time, duration, last_event, status, stabilize_time, replicate=r)

            # Create live data callback for real-time plotting
            def live_data_callback(sim_time, vitals, r=rep):
                with live_data_lock:
                    if job_id in live_data_subscribers:
                        vars_to_send = live_data_subscribers[job_id]
                        data_point = {'t': sim_time, 'replicate': r}
                        for var in vars_to_send:
                            if var in vitals:
                                data_point[var] = vitals[var]
                        socketio.emit('live_data', {'job_id': job_id, 'data': data_point})

            runner = SimulationRunner(job_id, job, rep_progress_callback, live_data_callback)
            csv_data = runner.run()

            if runner.cancelled:
                cancelled = True

            # Generate filename with replicate suffix if multiple replicates
            if total_replicates > 1:
                csv_path = os.path.join(RESULTS_FOLDER, f"{job['name']}_{timestamp}_r{rep}.csv")
            else:
                csv_path = os.path.join(RESULTS_FOLDER, f"{job['name']}_{timestamp}.csv")

            with open(csv_path, 'w', newline='') as f:
                f.write(csv_data)

            csv_paths.append(csv_path)
            total_samples += len(csv_data.strip().split('\n')) - 1

            if cancelled:
                break

        with job_lock:
            if cancelled:
                jobs[job_id]['status'] = 'cancelled'
            else:
                jobs[job_id]['status'] = 'complete'

            # Store paths - single path for 1 replicate, list for multiple
            if total_replicates == 1:
                jobs[job_id]['csv_path'] = csv_paths[0] if csv_paths else None
            else:
                jobs[job_id]['csv_paths'] = csv_paths
                jobs[job_id]['csv_path'] = csv_paths[0] if csv_paths else None  # For backwards compatibility

            jobs[job_id]['samples'] = total_samples
            jobs[job_id]['replicates_completed'] = len(csv_paths)

            # Clean up cancel flag
            if job_id in cancel_flags:
                del cancel_flags[job_id]

    except Exception as e:
        import traceback
        with job_lock:
            jobs[job_id]['status'] = 'error'
            jobs[job_id]['message'] = str(e)
            jobs[job_id]['traceback'] = traceback.format_exc()
            if job_id in cancel_flags:
                del cancel_flags[job_id]


@app.route('/')
def serve_gui():
    """Serve the GUI HTML file."""
    gui_path = os.path.join(SCRIPT_DIR, 'pulse_gui.html')
    
    if not os.path.exists(gui_path):
        return """
        <html><body style="font-family: sans-serif; padding: 40px;">
        <h1>Pulse Server Running</h1>
        <p>API is available at <code>/api/</code></p>
        <p>To use the GUI, place <code>pulse_gui.html</code> in the same directory as <code>pulse_server.py</code></p>
        <p>Or open <code>pulse_gui.html</code> directly in a browser and connect to this server.</p>
        </body></html>
        """, 200
    
    # Read the GUI HTML
    with open(gui_path, 'r', encoding='utf-8') as f:
        html = f.read()
    
    # Auto-set the server URL to this server's address
    # Replace the default localhost:8080 with empty string so it uses relative URLs
    html = html.replace('value="http://localhost:8080"', 'value=""')
    
    return html


@app.route('/api/info')
def api_info():
    """Return server info for GUI to configure itself."""
    patients = get_patients()
    cpu_count = os.cpu_count() or 4
    return jsonify({
        'patients': patients,
        'cpu_count': cpu_count,
        'version': '4.0',
        'pulse_home': PULSE_HOME
    })


@app.route('/api/available_variables')
def api_available_variables():
    """Return the list of available CSV output variables for the GUI."""
    return jsonify(AVAILABLE_VARIABLES)


@app.route('/api/submit_job', methods=['POST'])
def submit_job():
    job = request.json
    job_id = str(uuid.uuid4())[:8]

    patient_type = job.get('patient')
    
    # Determine if stabilization is needed
    # Custom patients (form) always need stabilization
    # Uploaded JSON might be pre-stabilized state or patient definition
    needs_stabilization = False
    if patient_type == '__custom__':
        needs_stabilization = True
    elif patient_type == '__upload__':
        # Check if uploaded JSON is a state file or patient definition
        patient_json = job.get('patient_json', {})
        is_state_file = any(key in patient_json for key in ['SimulationTime', 'InitialPatient', 'Compartments', 'CurrentPatient'])
        needs_stabilization = not is_state_file
    
    replicates = job.get('replicates', 1)
    with job_lock:
        jobs[job_id] = {
            'status': 'stabilizing' if needs_stabilization else 'running',
            'job': job,
            'sim_time': 0,
            'duration': job.get('duration_s', 300),
            'last_event': None,
            'stabilize_time': 0,
            'started': datetime.now().isoformat(),
            'current_replicate': 1,
            'total_replicates': replicates
        }
    
    thread = threading.Thread(target=run_job_thread, args=(job_id, job), daemon=True)
    thread.start()
    
    return jsonify({'job_id': job_id})


@app.route('/api/job_status/<job_id>')
def job_status(job_id):
    with job_lock:
        if job_id not in jobs:
            return jsonify({'status': 'not_found'}), 404
        return jsonify(jobs[job_id])


@app.route('/api/cancel_job/<job_id>', methods=['POST'])
def cancel_job(job_id):
    """Cancel a running job."""
    with job_lock:
        if job_id not in jobs:
            return jsonify({'success': False, 'error': 'Job not found'}), 404
        
        status = jobs[job_id].get('status')
        if status not in ('running', 'stabilizing'):
            return jsonify({'success': False, 'error': f'Job is {status}, cannot cancel'})
        
        cancel_flags[job_id] = True
        return jsonify({'success': True, 'message': 'Cancellation requested'})


@app.route('/api/download/<job_id>')
def download(job_id):
    with job_lock:
        if job_id not in jobs:
            return "Not found", 404
        job_info = jobs[job_id].copy()

    # Check for multiple replicates (returns ZIP) or single file (returns CSV)
    if 'csv_paths' in job_info and len(job_info['csv_paths']) > 1:
        # Multiple replicates - create a ZIP file
        job_name = job_info.get('job', {}).get('name', 'results')
        zip_path = os.path.join(RESULTS_FOLDER, f"{job_name}_replicates.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for csv_path in job_info['csv_paths']:
                if os.path.exists(csv_path):
                    zf.write(csv_path, os.path.basename(csv_path))
        return send_file(zip_path, as_attachment=True)
    elif 'csv_path' in job_info:
        return send_file(job_info['csv_path'], as_attachment=True)
    else:
        return "Not ready", 400


@app.route('/api/list_results')
def list_results():
    results = []
    
    with job_lock:
        for job_id, info in jobs.items():
            if info.get('status') == 'complete':
                job = info.get('job', {})
                results.append({
                    'job_id': job_id,
                    'type': 'single',
                    'name': job.get('name', 'unnamed'),
                    'patient': job.get('patient', '').replace('@0s.json', ''),
                    'duration': job.get('duration_s', 0),
                    'event_count': len(job.get('events', [])),
                    'date': info.get('started', '')[:19].replace('T', ' ')
                })
    
    # TODO: Add batch and vent mechanics results
    
    return jsonify(results)


@app.route('/api/test_http_controller', methods=['POST'])
def test_http_controller():
    """Test connection to an HTTP controller."""
    data = request.json
    url = data.get('url', '').strip()
    
    if not url:
        return jsonify({'success': False, 'error': 'No URL provided'})
    
    try:
        payload = {
            "patient": "TestPatient",
            "vent_settings": {"mode": "VC-AC", "fio2": 0.4, "peep_cmh2o": 5, "vt_ml": 420, "rr": 14},
            "config": {}
        }
        
        resp = http_requests.post(
            f"{url.rstrip('/')}/init",
            json=payload,
            timeout=5.0
        )
        resp.raise_for_status()
        result = resp.json()
        
        if result.get("status") != "ok":
            return jsonify({'success': False, 'error': f"Controller returned: {result.get('error', 'unknown error')}"})
        
        data_requests = result.get("data_requests", [])
        
        for req in data_requests:
            category = req.get("category")
            name = req.get("name")
            unit_str = req.get("unit")
            
            if category not in DATA_REQUEST_FACTORIES:
                return jsonify({
                    'success': False, 
                    'error': f"Invalid category '{category}' for {name}"
                })
            
            if unit_str and unit_str not in UNIT_MAP:
                return jsonify({
                    'success': False,
                    'error': f"Invalid unit '{unit_str}' for {category}:{name}"
                })
        
        return jsonify({
            'success': True,
            'data_requests': len(data_requests),
            'next_update_s': result.get("next_update_s", 1.0)
        })
        
    except http_requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': f'Connection timed out after 5s'})
    except http_requests.exceptions.ConnectionError as e:
        return jsonify({'success': False, 'error': f'Cannot connect to {url}'})
    except http_requests.exceptions.RequestException as e:
        return jsonify({'success': False, 'error': f'HTTP error: {e}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# === BATCH MODE SUPPORT ===
batches = {}
batch_lock = threading.Lock()
batch_cancel_flags = {}  # batch_id -> True if should cancel (for thread-level check)

# File-based cancellation for cross-process communication
# Using files instead of multiprocessing.Manager because Manager proxies
# don't reliably propagate updates to already-running worker processes

def get_cancel_flag_path(batch_id):
    """Get the path to the cancel flag file for a batch.

    Uses os.path.realpath() to normalize the temp directory path,
    ensuring consistency between main process and worker processes
    (Windows can return short 8.3 paths vs long paths depending on context).
    """
    temp_dir = os.path.realpath(tempfile.gettempdir())
    return os.path.join(temp_dir, f"pulse_batch_cancel_{batch_id}.flag")

def set_batch_cancel_flag(batch_id):
    """Set the cancel flag for a batch by creating a flag file."""
    flag_path = get_cancel_flag_path(batch_id)
    try:
        with open(flag_path, 'w') as f:
            f.write('cancel')
        # Also get the absolute/real path to verify
        real_path = os.path.realpath(flag_path)
        print(f"[BATCH] Cancel flag file created: {flag_path}")
        if real_path != flag_path:
            print(f"[BATCH] Real path: {real_path}")
        return True
    except Exception as e:
        print(f"[BATCH] Failed to create cancel flag file: {e}")
        return False

def check_batch_cancel_flag(batch_id):
    """Check if the cancel flag is set for a batch."""
    flag_path = get_cancel_flag_path(batch_id)
    return os.path.exists(flag_path)

def clear_batch_cancel_flag(batch_id):
    """Clear the cancel flag for a batch by removing the flag file."""
    flag_path = get_cancel_flag_path(batch_id)
    try:
        if os.path.exists(flag_path):
            os.remove(flag_path)
    except Exception:
        pass  # Ignore errors during cleanup


def _extract_batch_patient_weight(patient_json, patient_name, initial_blood_volume_ml):
    """Extract patient weight in kg for batch worker.

    Tries in order:
    1. patient_json state file (CurrentPatient.Weight)
    2. patient_json patient definition (Weight)
    3. Read from state file on disk
    4. Fallback: estimate from blood volume
    """
    weight_kg = None

    # 1. Patient JSON (state file or patient definition)
    if patient_json:
        # State file format: CurrentPatient.Weight.ScalarMass
        if "CurrentPatient" in patient_json:
            weight_data = patient_json.get("CurrentPatient", {}).get("Weight", {}).get("ScalarMass", {})
        else:
            # Patient definition format: Weight.ScalarMass
            weight_data = patient_json.get("Weight", {}).get("ScalarMass", {})

        if weight_data:
            value = weight_data.get("Value")
            unit = weight_data.get("Unit", "lb")
            if value:
                if unit == "kg":
                    weight_kg = value
                else:  # Default to lb
                    weight_kg = value * 0.453592

    # 2. Read from state file on disk
    elif patient_name:
        try:
            import json as json_module
            state_path = f"./states/{patient_name}" if not os.path.isabs(patient_name) else patient_name
            if os.path.exists(state_path):
                with open(state_path, 'r') as f:
                    state_data = json_module.load(f)
                weight_data = state_data.get("CurrentPatient", {}).get("Weight", {}).get("ScalarMass", {})
                if weight_data:
                    value = weight_data.get("Value")
                    unit = weight_data.get("Unit", "lb")
                    if value:
                        if unit == "kg":
                            weight_kg = value
                        else:
                            weight_kg = value * 0.453592
        except Exception:
            pass  # Fall through to estimation

    # 3. Fallback: estimate from blood volume
    if weight_kg is None or weight_kg < 20:
        weight_kg = initial_blood_volume_ml / 70.0
        if weight_kg < 20:
            weight_kg = 70.0  # Default adult weight

    return weight_kg


def run_single_patient(args):
    """
    Run a single patient simulation in a separate process.

    This function must be completely self-contained with no shared state,
    since it runs in a separate process (multiprocessing.Pool).

    Args can contain either:
    - patient: filename of pre-stabilized state (e.g., "Carol@0s.json")
    - patient_json: dict with 'name' and 'json' for custom patient

    Args tuple: (patient_info, batch_config, output_dir, job_id)
    - job_id is the unique identifier including replicate suffix (e.g., "Carol@0s.json_r1")
    - Cancellation is checked via file-based flag (check_batch_cancel_flag)
    """
    # Support various tuple formats for backwards compatibility
    if len(args) == 5:
        # Legacy format with shared_cancel dict (ignored now, we use file-based)
        patient_info, batch_config, output_dir, job_id, _ = args
    elif len(args) == 4:
        patient_info, batch_config, output_dir, job_id = args
    else:
        patient_info, batch_config, output_dir = args
        job_id = None  # Will be set below

    # Get batch_id for cancellation check (file-based)
    batch_id = batch_config.get('batch_id', '')
    
    duration_s = batch_config['duration_s']
    sample_rate_hz = batch_config.get('sample_rate_hz', 50)
    vent_settings = batch_config['vent_settings']
    events = sorted(batch_config.get('events', []), key=lambda e: e.get('time', 0))
    pulse_bin = batch_config['pulse_bin']
    pulse_python = batch_config.get('pulse_python', pulse_bin)
    
    timestep = 1.0 / sample_rate_hz
    
    # Get replicate suffix if present
    replicate_suffix = batch_config.get('replicate_suffix', '')

    # Determine if this is a pre-stabilized or custom patient
    if isinstance(patient_info, dict):
        patient_name = patient_info['name']
        patient_json = patient_info['json']
        is_custom = True
    else:
        patient_name = patient_info
        patient_json = None
        is_custom = False

    # Set job_id if not provided (legacy support)
    if job_id is None:
        if is_custom:
            job_id = f"custom_{patient_name}{replicate_suffix}"
        else:
            job_id = f"{patient_name}{replicate_suffix}"
    
    try:
        # Set up paths for this worker process (Windows spawn doesn't inherit sys.path)
        import sys
        for path in [pulse_python, pulse_bin]:
            if path not in sys.path:
                sys.path.insert(0, path)
        
        # Add DLL directory on Windows
        if hasattr(os, 'add_dll_directory'):
            try:
                os.add_dll_directory(pulse_bin)
            except OSError:
                pass  # Already added or doesn't exist
        
        os.chdir(pulse_bin)
        
        # Import Pulse in the worker process
        from pulse.engine.PulseEngine import PulseEngine
        from pulse.cdm.engine import SEDataRequest, SEDataRequestManager, IEventHandler, SEEventChange, eEvent
        from pulse.cdm.scalars import FrequencyUnit, PressureUnit, TimeUnit, VolumeUnit, VolumePerTimeUnit, LengthUnit, MassUnit, MassPerVolumeUnit, AmountPerVolumeUnit, VolumePerPressureUnit, PressureTimePerVolumeUnit
        from pulse.cdm.patient_actions import (
            SEIntubation, eIntubationType, SEExercise,
            SEAcuteRespiratoryDistressSyndromeExacerbation,
            SEAirwayObstruction, SEAcuteStress,
            SEHemorrhage, eHemorrhage_Compartment,
            SESubstanceBolus, SESubstanceInfusion,
            SESubstanceCompoundInfusion
        )
        from pulse.cdm.physiology import eLungCompartment
        from pulse.cdm.mechanical_ventilator_actions import (
            SEMechanicalVentilatorVolumeControl,
            SEMechanicalVentilatorPressureControl,
            eMechanicalVentilator_VolumeControlMode,
            eMechanicalVentilator_PressureControlMode
        )
        from pulse.cdm.mechanical_ventilator import eSwitch
        from pulse.cdm.patient import SEPatientConfiguration, eSex
        from pulse.cdm.io.patient import serialize_patient_from_file
        import tempfile
        
        # Local event handler for batch simulation (can't use main class due to multiprocessing)
        class BatchEventHandler(IEventHandler):
            DEATH_EVENTS = {eEvent.IrreversibleState, eEvent.CardiacArrest}
            CRITICAL_EVENTS = {
                eEvent.IrreversibleState: "Irreversible State (Death)",
                eEvent.CardiacArrest: "Cardiac Arrest",
                eEvent.CardiovascularCollapse: "Cardiovascular Collapse",
                eEvent.HypovolemicShock: "Hypovolemic Shock",
            }

            def __init__(self):
                super().__init__(active_events_only=False)
                self.is_dead = False
                self.death_time_s = None
                self.death_cause = None
                self.event_log = []

            def handle_event(self, change):
                event = change.event
                time_s = change.sim_time.get_value(TimeUnit.s) if change.sim_time.is_valid() else 0
                if event in self.DEATH_EVENTS and change.active and not self.is_dead:
                    self.is_dead = True
                    self.death_time_s = time_s
                    self.death_cause = self.CRITICAL_EVENTS.get(event, str(event))
                if event in self.CRITICAL_EVENTS:
                    self.event_log.append((time_s, self.CRITICAL_EVENTS[event], change.active))

        # Local HTTP controller for batch simulation (can't use main class due to multiprocessing)
        class BatchHTTPController:
            """HTTP-based controller for batch simulation workers."""

            # Unit mapping (must be defined locally for multiprocessing)
            UNIT_MAP = {
                "mmHg": PressureUnit.mmHg,
                "cmH2O": PressureUnit.cmH2O,
                "Pa": PressureUnit.Pa,
                "mL": VolumeUnit.mL,
                "L": VolumeUnit.L,
                "mL/min": VolumePerTimeUnit.mL_Per_min,
                "L/min": VolumePerTimeUnit.L_Per_min,
                "1/min": FrequencyUnit.Per_min,
                "/min": FrequencyUnit.Per_min,
                "Per_min": FrequencyUnit.Per_min,
                "Hz": FrequencyUnit.Hz,
                "s": TimeUnit.s,
                "min": TimeUnit.min,
                "g": MassUnit.g,
                "mg": MassUnit.mg,
                "g/L": MassPerVolumeUnit.g_Per_L,
                "mg/mL": MassPerVolumeUnit.g_Per_L,
            }

            def __init__(self, base_url, config=None, timeout=10.0, simulation_context=None):
                import requests
                self.requests = requests
                self.base_url = base_url.rstrip('/')
                self.config = config or {}
                self.timeout = timeout
                self.data_requests = []
                self.next_update_s = 1.0
                self.initialized = False
                self.last_error = None
                # Simulation context for concurrent batch identification
                self.simulation_context = simulation_context or {}
                self.simulation_id = self.simulation_context.get('simulation_id', '')
                self.batch_id = self.simulation_context.get('batch_id', '')
                self.job_id = self.simulation_context.get('job_id', '')

            def send_init(self, patient, vent_settings):
                payload = {
                    "patient": patient,
                    "vent_settings": vent_settings,
                    "config": self.config,
                    # Simulation identifiers for concurrent batch support
                    "simulation_id": self.simulation_id,
                    "batch_id": self.batch_id,
                    "job_id": self.job_id
                }

                try:
                    resp = self.requests.post(
                        f"{self.base_url}/init",
                        json=payload,
                        timeout=self.timeout
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    if data.get("status") != "ok":
                        raise RuntimeError(f"Controller init failed: {data.get('error', 'unknown error')}")

                    self.data_requests = data.get("data_requests", [])
                    pulse_requests = []

                    for req in self.data_requests:
                        category = req.get("category")
                        name = req.get("name")
                        unit_str = req.get("unit")

                        if category == "Physiology":
                            factory = SEDataRequest.create_physiology_request
                        elif category == "MechanicalVentilator":
                            factory = SEDataRequest.create_mechanical_ventilator_request
                        else:
                            raise ValueError(f"Unknown data request category: '{category}'")

                        if unit_str:
                            if unit_str not in self.UNIT_MAP:
                                raise ValueError(f"Unknown unit: '{unit_str}' for {category}:{name}")
                            unit = self.UNIT_MAP[unit_str]
                            pulse_requests.append(factory(name, unit=unit))
                        else:
                            pulse_requests.append(factory(name))

                    self.next_update_s = data.get("next_update_s", 1.0)
                    self.initialized = True
                    self.last_error = None

                    return pulse_requests

                except self.requests.exceptions.RequestException as e:
                    self.last_error = f"HTTP error during init: {e}"
                    raise RuntimeError(self.last_error)
                except (KeyError, ValueError) as e:
                    self.last_error = f"Invalid controller response: {e}"
                    raise RuntimeError(self.last_error)

            def step(self, data_values, current_settings):
                if not self.initialized:
                    return None

                sim_time = data_values.pop("sim_time_s", 0.0)

                payload = {
                    "sim_time_s": sim_time,
                    "data": data_values,
                    # Simulation identifiers for concurrent batch support
                    "simulation_id": self.simulation_id,
                    "batch_id": self.batch_id,
                    "job_id": self.job_id
                }

                try:
                    resp = self.requests.post(
                        f"{self.base_url}/update",
                        json=payload,
                        timeout=self.timeout
                    )
                    resp.raise_for_status()
                    response = resp.json()

                    if "next_update_s" in response:
                        self.next_update_s = max(0.02, response["next_update_s"])

                    commands = response.get("commands", {})
                    result = current_settings.copy()
                    result.update(commands)
                    result["next_interval_s"] = self.next_update_s

                    self.last_error = None
                    return result

                except self.requests.exceptions.Timeout:
                    self.last_error = f"Controller timeout after {self.timeout}s"
                    print(f"[BatchHTTPController] {self.last_error}")
                    return None
                except self.requests.exceptions.RequestException as e:
                    self.last_error = f"HTTP error: {e}"
                    print(f"[BatchHTTPController] {self.last_error}")
                    return None
                except Exception as e:
                    self.last_error = f"Error parsing response: {e}"
                    print(f"[BatchHTTPController] {self.last_error}")
                    return None

            def shutdown(self):
                if not self.initialized:
                    return
                try:
                    payload = {
                        "simulation_id": self.simulation_id,
                        "batch_id": self.batch_id,
                        "job_id": self.job_id
                    }
                    self.requests.post(f"{self.base_url}/shutdown", json=payload, timeout=2.0)
                except:
                    pass

        # Local built-in controller for batch simulation
        class BatchBuiltinController:
            """Built-in controller implementations for batch simulation."""

            RANDOM_WALK_BOUNDS = {
                'fio2': {'min': 0.21, 'max': 1.0, 'step': 0.05, 'default': 0.4},
                'peep_cmh2o': {'min': 5, 'max': 20, 'step': 2, 'default': 5},
                'vt_ml': {'min': 300, 'max': 600, 'step': 25, 'default': 420},
                'rr': {'min': 10, 'max': 30, 'step': 2, 'default': 14},
                'itime_s': {'min': 0.8, 'max': 1.5, 'step': 0.1, 'default': 1.0},
                'pinsp_cmh2o': {'min': 10, 'max': 30, 'step': 2, 'default': 15},
            }

            def __init__(self, controller_type):
                self.controller_type = controller_type
                self.initialized = False

            def send_init(self, patient, vent_settings):
                self.initialized = True
                return []

            def step(self, vitals, current_settings):
                import random
                if self.controller_type == "ARDSNet":
                    return self._ardsnet_step(vitals, current_settings)
                elif self.controller_type == "Adaptive":
                    return self._adaptive_step(vitals, current_settings)
                elif self.controller_type == "Random":
                    return self._random_step(vitals, current_settings)
                return current_settings

            def _ardsnet_step(self, vitals, settings):
                result = settings.copy()
                spo2 = vitals.get('spo2_pct', 95)
                if spo2 < 88:
                    result['fio2'] = min(1.0, settings.get('fio2', 0.4) + 0.1)
                elif spo2 > 95:
                    result['fio2'] = max(0.21, settings.get('fio2', 0.4) - 0.05)
                return result

            def _adaptive_step(self, vitals, settings):
                result = settings.copy()
                spo2 = vitals.get('spo2_pct', 95)
                paco2 = vitals.get('paco2_mmhg', 40)
                if spo2 < 90:
                    result['fio2'] = min(1.0, settings.get('fio2', 0.4) + 0.1)
                    result['peep_cmh2o'] = min(20, settings.get('peep_cmh2o', 5) + 2)
                elif spo2 > 96:
                    result['fio2'] = max(0.21, settings.get('fio2', 0.4) - 0.05)
                if paco2 > 50:
                    result['rr'] = min(30, settings.get('rr', 14) + 2)
                elif paco2 < 35:
                    result['rr'] = max(8, settings.get('rr', 14) - 2)
                return result

            def _random_step(self, vitals, settings):
                import random
                result = settings.copy()
                param = random.choice(list(self.RANDOM_WALK_BOUNDS.keys()))
                bounds = self.RANDOM_WALK_BOUNDS[param]
                current = settings.get(param, bounds['default'])
                direction = random.choice([-1, 1])
                new_val = current + direction * bounds['step']
                new_val = max(bounds['min'], min(bounds['max'], new_val))
                result[param] = new_val
                return result

            def shutdown(self):
                pass

        # Local built-in fluid controller for batch simulation
        class BatchBuiltinFluidController:
            """Built-in fluid resuscitation controller for batch simulation."""

            def __init__(self, name):
                self.name = name
                self.settings = {
                    'crystalloid_rate_ml_min': 0,
                    'blood_rate_ml_min': 0,
                    'crystalloid_compound': 'Saline',
                    'blood_compound': 'Blood'
                }
                self.state = {
                    'phase': 'monitoring',
                    'last_map': None,
                    'last_hr': None,
                    'trend_improving': False
                }

            def send_init(self, patient, initial_settings=None):
                self.state['patient'] = patient
                if initial_settings:
                    self.settings.update(initial_settings)

            def step(self, vitals, current_fluid_settings, blood_loss_ml=0, blood_infused_ml=0, crystalloid_infused_ml=0):
                if self.name == 'default_fluid_controller':
                    return self._simple_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
                elif self.name == 'aggressive_fluid_controller':
                    return self._aggressive_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
                elif self.name == 'conservative_fluid_controller':
                    return self._conservative_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
                elif self.name == 'damage_control_fluid_controller':
                    return self._damage_control_resuscitation(vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml)
                return None

            def _simple_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
                """Simple MAP-based fluid resuscitation (MAP target 65-75)."""
                new = self.settings.copy()
                map_mmhg = vitals.get('map_mmhg', 70)
                hr_bpm = vitals.get('hr_bpm', 80)

                if self.state['last_map'] is not None:
                    self.state['trend_improving'] = map_mmhg > self.state['last_map']
                self.state['last_map'] = map_mmhg
                self.state['last_hr'] = hr_bpm

                if map_mmhg < 55:
                    self.state['phase'] = 'resuscitating'
                    new['crystalloid_rate_ml_min'] = 250
                    new['blood_rate_ml_min'] = 150 if blood_loss_ml > 500 else 0
                    new['next_interval_s'] = 10
                elif map_mmhg < 65:
                    self.state['phase'] = 'resuscitating'
                    current_crystalloid = self.settings.get('crystalloid_rate_ml_min', 0)
                    new['crystalloid_rate_ml_min'] = min(200, current_crystalloid + 50)
                    if blood_loss_ml > 750 and (blood_loss_ml - blood_infused_ml - crystalloid_infused_ml/3) > 500:
                        new['blood_rate_ml_min'] = 100
                    else:
                        new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 25)
                    new['next_interval_s'] = 15
                elif map_mmhg > 75:
                    self.state['phase'] = 'maintaining'
                    new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 50)
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
                    new['next_interval_s'] = 30
                else:
                    self.state['phase'] = 'maintaining'
                    new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 25)
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 25)
                    new['next_interval_s'] = 20

                if hr_bpm > 120 and map_mmhg < 70:
                    new['crystalloid_rate_ml_min'] = max(new['crystalloid_rate_ml_min'], 150)
                    new['next_interval_s'] = min(new['next_interval_s'], 15)

                self.settings = new.copy()
                return new

            def _aggressive_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
                """Aggressive fluid resuscitation - higher rates, earlier blood."""
                new = self.settings.copy()
                map_mmhg = vitals.get('map_mmhg', 70)

                if map_mmhg < 60:
                    new['crystalloid_rate_ml_min'] = 300
                    new['blood_rate_ml_min'] = 200 if blood_loss_ml > 300 else 100
                    new['next_interval_s'] = 5
                elif map_mmhg < 70:
                    new['crystalloid_rate_ml_min'] = 200
                    new['blood_rate_ml_min'] = 100 if blood_loss_ml > 500 else 0
                    new['next_interval_s'] = 10
                elif map_mmhg > 80:
                    new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 100)
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 100)
                    new['next_interval_s'] = 30
                else:
                    new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 50)
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
                    new['next_interval_s'] = 20

                self.settings = new.copy()
                return new

            def _conservative_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
                """Conservative/permissive hypotension - lower MAP targets (50-60)."""
                new = self.settings.copy()
                map_mmhg = vitals.get('map_mmhg', 70)

                if map_mmhg < 50:
                    new['crystalloid_rate_ml_min'] = 150
                    new['blood_rate_ml_min'] = 100 if blood_loss_ml > 1000 else 0
                    new['next_interval_s'] = 15
                elif map_mmhg < 55:
                    new['crystalloid_rate_ml_min'] = 100
                    new['blood_rate_ml_min'] = 50 if blood_loss_ml > 750 else 0
                    new['next_interval_s'] = 20
                elif map_mmhg > 65:
                    new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 50)
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
                    new['next_interval_s'] = 45
                else:
                    new['crystalloid_rate_ml_min'] = max(0, self.settings.get('crystalloid_rate_ml_min', 0) - 25)
                    new['blood_rate_ml_min'] = 0
                    new['next_interval_s'] = 30

                self.settings = new.copy()
                return new

            def _damage_control_resuscitation(self, vitals, blood_loss_ml, blood_infused_ml, crystalloid_infused_ml):
                """Damage control - prioritize blood, limit crystalloid to 2L."""
                new = self.settings.copy()
                map_mmhg = vitals.get('map_mmhg', 70)
                max_crystalloid = max(0, 2000 - crystalloid_infused_ml)

                if map_mmhg < 50:
                    new['blood_rate_ml_min'] = 200
                    new['crystalloid_rate_ml_min'] = min(100, max_crystalloid / 10) if max_crystalloid > 0 else 0
                    new['next_interval_s'] = 10
                elif map_mmhg < 60:
                    new['blood_rate_ml_min'] = 150
                    new['crystalloid_rate_ml_min'] = min(50, max_crystalloid / 20) if max_crystalloid > 0 else 0
                    new['next_interval_s'] = 15
                elif map_mmhg > 65:
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 50)
                    new['crystalloid_rate_ml_min'] = 0
                    new['next_interval_s'] = 30
                else:
                    new['blood_rate_ml_min'] = max(0, self.settings.get('blood_rate_ml_min', 0) - 25)
                    new['crystalloid_rate_ml_min'] = 0
                    new['next_interval_s'] = 20

                self.settings = new.copy()
                return new

            def shutdown(self):
                pass

        class BatchHTTPFluidController:
            """HTTP-based fluid controller for batch simulation workers."""

            def __init__(self, base_url, config=None, timeout=10.0, simulation_context=None):
                import requests
                self.requests = requests
                self.base_url = base_url.rstrip('/')
                self.config = config or {}
                self.timeout = timeout
                self.data_requests = []
                self.next_update_s = 10.0
                self.initialized = False
                self.last_error = None
                # Simulation context for concurrent batch identification
                self.simulation_context = simulation_context or {}
                self.simulation_id = self.simulation_context.get('simulation_id', '')
                self.batch_id = self.simulation_context.get('batch_id', '')
                self.job_id = self.simulation_context.get('job_id', '')

            def send_init(self, patient, fluid_settings):
                payload = {
                    "patient": patient,
                    "fluid_settings": fluid_settings,
                    "config": self.config,
                    "simulation_id": self.simulation_id,
                    "batch_id": self.batch_id,
                    "job_id": self.job_id
                }

                try:
                    resp = self.requests.post(
                        f"{self.base_url}/init",
                        json=payload,
                        timeout=self.timeout
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    if data.get("status") != "ok":
                        raise RuntimeError(f"Fluid controller init failed: {data.get('error', 'unknown error')}")

                    self.data_requests = data.get("data_requests", [])
                    self.next_update_s = data.get("next_update_s", 10.0)
                    self.initialized = True
                    self.last_error = None

                    return self.data_requests

                except self.requests.exceptions.RequestException as e:
                    self.last_error = f"HTTP error during init: {e}"
                    raise RuntimeError(self.last_error)
                except (KeyError, ValueError) as e:
                    self.last_error = f"Invalid controller response: {e}"
                    raise RuntimeError(self.last_error)

            def step(self, vitals, current_settings, blood_loss_ml=0, blood_infused_ml=0, crystalloid_infused_ml=0):
                if not self.initialized:
                    return None

                sim_time = vitals.pop("sim_time_s", 0.0) if "sim_time_s" in vitals else 0.0

                payload = {
                    "sim_time_s": sim_time,
                    "data": vitals,
                    "blood_loss_ml": blood_loss_ml,
                    "blood_infused_ml": blood_infused_ml,
                    "crystalloid_infused_ml": crystalloid_infused_ml,
                    "simulation_id": self.simulation_id,
                    "batch_id": self.batch_id,
                    "job_id": self.job_id
                }

                try:
                    resp = self.requests.post(
                        f"{self.base_url}/update",
                        json=payload,
                        timeout=self.timeout
                    )
                    resp.raise_for_status()
                    response = resp.json()

                    if "next_update_s" in response:
                        self.next_update_s = max(1.0, response["next_update_s"])

                    commands = response.get("commands", {})
                    result = current_settings.copy()
                    result.update(commands)
                    result["next_interval_s"] = self.next_update_s

                    self.last_error = None
                    return result

                except self.requests.exceptions.Timeout:
                    self.last_error = f"Fluid controller timeout after {self.timeout}s"
                    print(f"[BatchHTTPFluidController] {self.last_error}")
                    return None
                except self.requests.exceptions.RequestException as e:
                    self.last_error = f"HTTP error: {e}"
                    print(f"[BatchHTTPFluidController] {self.last_error}")
                    return None
                except Exception as e:
                    self.last_error = f"Error parsing response: {e}"
                    print(f"[BatchHTTPFluidController] {self.last_error}")
                    return None

            def shutdown(self):
                if not self.initialized:
                    return
                try:
                    payload = {
                        "simulation_id": self.simulation_id,
                        "batch_id": self.batch_id,
                        "job_id": self.job_id
                    }
                    self.requests.post(f"{self.base_url}/shutdown", json=payload, timeout=2.0)
                except:
                    pass

        # Create engine for this patient
        pulse = PulseEngine()
        safe_name = patient_name.replace('@', '_').replace('.', '_').replace(' ', '_')
        pulse.set_log_filename(f"./test_results/batch_{safe_name}.log")
        pulse.log_to_console(False)

        # Set up event handler for death detection
        event_handler = BatchEventHandler()
        pulse.set_event_handler(event_handler)
        
        # Build data requests dynamically from selected output columns
        avail_vars = batch_config.get('available_variables', [])
        output_columns = batch_config.get('output_columns')
        if output_columns is not None and avail_vars:
            selected_set = set(output_columns)
            selected_vars = [v for v in avail_vars if v['key'] in selected_set]
        elif avail_vars:
            selected_vars = [v for v in avail_vars if v.get('default')]
        else:
            selected_vars = []

        # Unit string -> Pulse unit object mapping (needed in worker process)
        worker_unit_map = {
            "FrequencyUnit.Per_min": FrequencyUnit.Per_min,
            "PressureUnit.mmHg": PressureUnit.mmHg,
            "PressureUnit.cmH2O": PressureUnit.cmH2O,
            "VolumeUnit.mL": VolumeUnit.mL,
            "VolumePerTimeUnit.L_Per_min": VolumePerTimeUnit.L_Per_min,
            "VolumePerTimeUnit.mL_Per_min": VolumePerTimeUnit.mL_Per_min,
            "MassPerVolumeUnit.g_Per_L": MassPerVolumeUnit.g_Per_L,
            "VolumePerPressureUnit.mL_Per_cmH2O": VolumePerPressureUnit.mL_Per_cmH2O,
            "PressureTimePerVolumeUnit.cmH2O_s_Per_L": PressureTimePerVolumeUnit.cmH2O_s_Per_L,
        }

        data_requests = []
        for var in selected_vars:
            unit = worker_unit_map.get(var.get("pulse_unit")) if var.get("pulse_unit") else None
            kwargs = {}
            if unit is not None:
                kwargs["unit"] = unit
            if var["request_type"] == "physiology":
                data_requests.append(SEDataRequest.create_physiology_request(var["pulse_name"], **kwargs))
            elif var["request_type"] == "mechanical_ventilator":
                data_requests.append(SEDataRequest.create_mechanical_ventilator_request(var["pulse_name"], **kwargs))
            elif var["request_type"] == "compartment_substance":
                data_requests.append(
                    SEDataRequest.create_liquid_compartment_substance_request(
                        var["compartment"], var["pulse_name"], var["property"], **kwargs
                    )
                )
        data_mgr = SEDataRequestManager(data_requests)
        
        if is_custom:
            # Check if this is a pre-stabilized state file or a patient definition
            is_state_file = any(key in patient_json for key in ['SimulationTime', 'InitialPatient', 'Compartments', 'CurrentPatient'])
            
            if is_state_file:
                # Pre-stabilized state file - load directly
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, dir=PULSE_BIN) as f:
                    import json as json_module
                    json_module.dump(patient_json, f)
                    temp_path = f.name
                
                try:
                    rel_path = os.path.basename(temp_path)
                    if not pulse.serialize_from_file(rel_path, data_mgr):
                        raise RuntimeError(f"Failed to load pre-stabilized state: {patient_name}")
                finally:
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
            else:
                # Patient definition - needs stabilization
                pc = SEPatientConfiguration()
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    import json as json_module
                    json_module.dump(patient_json, f)
                    temp_path = f.name
                
                try:
                    serialize_patient_from_file(temp_path, pc.get_patient())
                    pc.set_data_root_dir("./")
                    if not pulse.initialize_engine(pc, data_mgr):
                        raise RuntimeError(f"Failed to stabilize custom patient: {patient_name}")
                finally:
                    os.unlink(temp_path)
        else:
            # Pre-stabilized patient - load state directly
            patient_path = f"./states/{patient_name}"
            if not pulse.serialize_from_file(patient_path, data_mgr):
                raise RuntimeError(f"Failed to load patient: {patient_name}")

        # Capture initial blood volume for relative hemorrhage rates
        # Advance time slightly to ensure engine has computed state values
        pulse.advance_time_s(0.1)
        initial_results = pulse.pull_data()
        # Use dynamic vitals dict to get blood volume by key (index depends on selected columns)
        initial_vitals = {"t": initial_results[0]}
        for vi, var in enumerate(selected_vars):
            val = initial_results[vi + 1]
            transform = var.get("transform")
            if transform == "multiply_100":
                val = val * 100
            elif transform == "lactate_g_to_mmol":
                val = val * 11.1
            initial_vitals[var["key"]] = val
        initial_blood_volume_ml = initial_vitals.get("blood_volume_ml", 5000.0)
        if initial_blood_volume_ml <= 0:
            initial_blood_volume_ml = 5000.0

        # Extract patient weight for weight-based drug dosing
        patient_weight_kg = _extract_batch_patient_weight(patient_json, patient_name, initial_blood_volume_ml)

        # Track intubation and vent state
        is_intubated = False
        vent_active = False
        # v6: start_intubated defaults to False - use events for intubation/ventilation
        start_intubated = batch_config.get('start_intubated', False)

        if start_intubated:
            # Legacy support: intubate at t=0 if start_intubated is True
            intubation = SEIntubation()
            intubation.set_type(eIntubationType.Tracheal)
            pulse.process_action(intubation)
            pulse.advance_time_s(2)
            is_intubated = True

            # Start ventilator with job-level settings
            vent = SEMechanicalVentilatorVolumeControl()
            vent.set_connection(eSwitch.On)
            vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
            vent.get_tidal_volume().set_value(vent_settings.get('vt_ml', 420), VolumeUnit.mL)
            vent.get_respiration_rate().set_value(vent_settings.get('rr', 14), FrequencyUnit.Per_min)
            vent.get_fraction_inspired_oxygen().set_value(vent_settings.get('fio2', 0.4))
            vent.get_positive_end_expired_pressure().set_value(vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
            vent.get_inspiratory_period().set_value(vent_settings.get('itime_s', 1.0), TimeUnit.s)
            vent.get_flow().set_value(vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
            pulse.process_action(vent)
            pulse.advance_time_s(3)
            vent_active = True
        
        # CSV output - build columns dynamically from selected variables
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        csv_columns = ["sim_time_s"]
        csv_columns.extend([v["key"] for v in selected_vars])
        csv_columns.extend([
            "cmd_mode", "cmd_vt_ml", "cmd_rr", "cmd_fio2",
            "cmd_peep_cmh2o", "cmd_pinsp_cmh2o", "cmd_itime_s",
            "is_intubated", "vent_active", "controller_active", "fluid_controller_active",
            "blood_loss_ml", "blood_infused_ml", "crystalloid_infused_ml",
            "cmd_crystalloid_rate", "cmd_blood_rate",
            "event", "controller_cmd", "fluid_cmd"
        ])
        writer.writerow(csv_columns)
        
        # Run simulation
        sim_time = 0.0
        event_idx = 0
        patient_dead = False

        # Track hemorrhage conditions (for conditional stop feature)
        hemorrhage_stop_conditions = {}  # compartment -> {vital, operator, value, maxDurationSec, startTime}
        hemorrhage_flow_rates = {}  # compartment -> current flow rate

        # Ventilator controller state
        controller = None
        controller_active = False
        last_control_time = 0.0
        control_interval_s = 1.0
        controller_cmd = ""  # For CSV output

        # Fluid controller state
        fluid_controller = None
        fluid_controller_active = False
        last_fluid_control_time = 0.0
        fluid_control_interval_s = 10.0
        fluid_controller_cmd = ""  # For CSV output
        fluid_settings = {
            'crystalloid_rate_ml_min': 0,
            'blood_rate_ml_min': 0,
            'crystalloid_compound': 'Saline',
            'blood_compound': 'Blood'
        }

        # Fluid infusion tracking
        cumulative_blood_loss_ml = 0.0
        cumulative_blood_infused_ml = 0.0
        cumulative_crystalloid_infused_ml = 0.0
        active_infusions = {}  # compound -> {rate, remaining_ml, is_blood}

        # Separate trigger-based events from time-based events
        time_events = [e for e in events if "trigger" not in e]
        triggered_events = [e for e in events if "trigger" in e]
        fired_trigger_ids = set()
        fired_event_names = set()  # Track event names for after_event triggers
        pending_delayed_triggers = {}  # event_id -> fire_time for triggers waiting on delay

        # Use time_events for the main loop
        events = time_events

        # Track if we were cancelled
        was_cancelled = False
        last_cancel_check = 0  # Check cancellation every 5 seconds of sim time
        cancel_check_count = 0


        last_heartbeat = 0
        while sim_time < duration_s:
            # Check for cancellation periodically (every 5 seconds of sim time)
            if (sim_time - last_cancel_check) >= 5.0:
                last_cancel_check = sim_time

                # Check for cancellation using file-based flag
                # Use os.path.realpath() to normalize path (Windows short vs long path issue)
                if batch_id:
                    temp_dir = os.path.realpath(tempfile.gettempdir())
                    cancel_flag_path = os.path.join(temp_dir, f"pulse_batch_cancel_{batch_id}.flag")
                    if os.path.exists(cancel_flag_path):
                        print(f"[Batch {job_id}] Cancelled at t={sim_time:.1f}s")
                        was_cancelled = True
                        break

            # Process events at this time - collect all that fire now
            current_events = []
            while event_idx < len(events) and events[event_idx].get('time', 0) <= sim_time:
                event = events[event_idx]
                etype = event.get('type', '')
                event_name = ""
                
                if etype == 'pathology':
                    pathology = event.get('pathology', 'ARDS')
                    severity = event.get('severity', 0.5)
                    
                    if pathology == 'ARDS':
                        action = SEAcuteRespiratoryDistressSyndromeExacerbation()
                        action.get_severity(eLungCompartment.LeftLung).set_value(severity)
                        action.get_severity(eLungCompartment.RightLung).set_value(severity)
                        pulse.process_action(action)
                        event_name = f"ARDS({severity})"
                    elif pathology == 'AirwayObstruction':
                        action = SEAirwayObstruction()
                        action.get_severity().set_value(severity)
                        pulse.process_action(action)
                        event_name = f"AirwayObs({severity})"
                    elif pathology == 'AcuteStress':
                        action = SEAcuteStress()
                        action.get_severity().set_value(severity)
                        pulse.process_action(action)
                        event_name = f"Stress({severity})"
                    elif pathology == 'Hemorrhage':
                        compartment = event.get('compartment', 'VenaCava')
                        flow_rate = event.get('flowRate', event.get('flow_rate', 100))
                        flow_rate_mode = event.get('flowRateMode', 'absolute')
                        is_auto_stop = event.get('isAutoStop', False)

                        # Convert percentage of blood volume to absolute mL/min if needed
                        if flow_rate_mode == "percent_bv" and initial_blood_volume_ml > 0:
                            absolute_rate = (flow_rate / 100.0) * initial_blood_volume_ml
                            flow_rate = absolute_rate

                        # Map compartment string to enum
                        compartment_map = {
                            "RightLeg": eHemorrhage_Compartment.RightLeg,
                            "LeftLeg": eHemorrhage_Compartment.LeftLeg,
                            "RightArm": eHemorrhage_Compartment.RightArm,
                            "LeftArm": eHemorrhage_Compartment.LeftArm,
                            "Aorta": eHemorrhage_Compartment.Aorta,
                            "VenaCava": eHemorrhage_Compartment.VenaCava,
                        }

                        action = SEHemorrhage()
                        action.set_compartment(compartment_map.get(compartment, eHemorrhage_Compartment.VenaCava))
                        action.get_flow_rate().set_value(flow_rate, VolumePerTimeUnit.mL_Per_min)
                        pulse.process_action(action)

                        hemorrhage_flow_rates[compartment] = flow_rate

                        # Register stop condition if this is a conditional hemorrhage
                        stop_condition = event.get('stopCondition')
                        if stop_condition and flow_rate > 0:
                            hemorrhage_stop_conditions[compartment] = {
                                "vital": stop_condition.get("vital", "hr_bpm"),
                                "operator": stop_condition.get("operator", ">="),
                                "value": stop_condition.get("value", 120),
                                "maxDurationSec": event.get("maxDurationSec", 600),
                                "startTime": sim_time
                            }

                        if flow_rate == 0 or is_auto_stop:
                            if compartment in hemorrhage_stop_conditions:
                                del hemorrhage_stop_conditions[compartment]
                            event_name = f"StopHemorrhage({compartment})"
                        else:
                            event_name = f"Hemorrhage({compartment},{flow_rate:.1f}mL/min)"
                
                # Legacy pathology event types for backwards compatibility
                elif etype == 'pathology_ards':
                    severity = event.get('severity', 0.5)
                    action = SEAcuteRespiratoryDistressSyndromeExacerbation()
                    action.get_severity(eLungCompartment.LeftLung).set_value(severity)
                    action.get_severity(eLungCompartment.RightLung).set_value(severity)
                    pulse.process_action(action)
                    event_name = f"ARDS({severity})"
                elif etype == 'pathology_airway':
                    severity = event.get('severity', 0.5)
                    action = SEAirwayObstruction()
                    action.get_severity().set_value(severity)
                    pulse.process_action(action)
                    event_name = f"AirwayObs({severity})"
                elif etype == 'pathology_stress':
                    severity = event.get('severity', 0.5)
                    action = SEAcuteStress()
                    action.get_severity().set_value(severity)
                    pulse.process_action(action)
                    event_name = f"Stress({severity})"
                
                elif etype == 'intubate':
                    intubation_type = event.get('intubationType', 'Tracheal')
                    type_map = {
                        'Tracheal': eIntubationType.Tracheal,
                        'RightMainstem': eIntubationType.RightMainstem,
                        'LeftMainstem': eIntubationType.LeftMainstem,
                        'Esophageal': eIntubationType.Esophageal,
                        'Oropharyngeal': eIntubationType.Oropharyngeal,
                        'Nasopharyngeal': eIntubationType.Nasopharyngeal,
                    }
                    intubation = SEIntubation()
                    intubation.set_type(type_map.get(intubation_type, eIntubationType.Tracheal))
                    pulse.process_action(intubation)
                    is_intubated = (intubation_type != 'Off')
                    event_name = f"Intubate({intubation_type})"
                
                elif etype == 'start_vent':
                    # v6: vent_settings can be embedded in the event
                    evt_vent_settings = event.get('vent_settings', vent_settings)
                    vent = SEMechanicalVentilatorVolumeControl()
                    vent.set_connection(eSwitch.On)
                    vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
                    vent.get_tidal_volume().set_value(evt_vent_settings.get('vt_ml', 420), VolumeUnit.mL)
                    vent.get_respiration_rate().set_value(evt_vent_settings.get('rr', 14), FrequencyUnit.Per_min)
                    vent.get_fraction_inspired_oxygen().set_value(evt_vent_settings.get('fio2', 0.4))
                    vent.get_positive_end_expired_pressure().set_value(evt_vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
                    vent.get_inspiratory_period().set_value(evt_vent_settings.get('itime_s', 1.0), TimeUnit.s)
                    vent.get_flow().set_value(evt_vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
                    pulse.process_action(vent)
                    vent_active = True
                    # Update job-level settings for CSV output
                    vent_settings.update(evt_vent_settings)
                    event_name = "StartVent"

                elif etype == 'change_vent':
                    # v6: Change ventilator settings mid-simulation
                    evt_vent_settings = event.get('vent_settings', {})
                    if evt_vent_settings:
                        vent = SEMechanicalVentilatorVolumeControl()
                        vent.set_connection(eSwitch.On)
                        vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
                        vent.get_tidal_volume().set_value(evt_vent_settings.get('vt_ml', 420), VolumeUnit.mL)
                        vent.get_respiration_rate().set_value(evt_vent_settings.get('rr', 14), FrequencyUnit.Per_min)
                        vent.get_fraction_inspired_oxygen().set_value(evt_vent_settings.get('fio2', 0.4))
                        vent.get_positive_end_expired_pressure().set_value(evt_vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
                        vent.get_inspiratory_period().set_value(evt_vent_settings.get('itime_s', 1.0), TimeUnit.s)
                        vent.get_flow().set_value(evt_vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
                        pulse.process_action(vent)
                        # Update job-level settings for CSV output
                        vent_settings.update(evt_vent_settings)
                        event_name = "ChangeVent"

                elif etype == 'exercise':
                    intensity = event.get('intensity', event.get('severity', 0.5))
                    action = SEExercise()
                    action.get_intensity().set_value(intensity)
                    pulse.process_action(action)
                    event_name = f"Exercise({intensity})"
                
                elif etype == 'bolus':
                    drug = event.get('drug', 'Rocuronium')
                    dose_mode = event.get('dose_mode', 'fixed')
                    concentration = float(event.get('concentration', 10))
                    conc_unit = event.get('concentration_unit', 'mg/mL')

                    # Calculate dose based on mode
                    if dose_mode == 'weight':
                        dose_per_kg = float(event.get('dose_per_kg', 0.5))
                        dose_per_kg_unit = event.get('dose_per_kg_unit', 'mg/kg')

                        # Calculate total mass in mg based on patient weight
                        if dose_per_kg_unit in ('ug/kg', 'mcg/kg'):
                            total_mass_mg = (dose_per_kg / 1000.0) * patient_weight_kg
                        else:  # mg/kg
                            total_mass_mg = dose_per_kg * patient_weight_kg

                        # Calculate volume from total mass and concentration
                        if conc_unit == 'mg/mL':
                            conc_mg_per_mL = concentration
                        elif conc_unit == 'ug/mL':
                            conc_mg_per_mL = concentration / 1000.0
                        elif conc_unit == 'g/L':
                            conc_mg_per_mL = concentration
                        else:
                            conc_mg_per_mL = concentration

                        dose_mL = total_mass_mg / conc_mg_per_mL if conc_mg_per_mL > 0 else 1.0
                        dose_desc = f"{dose_per_kg}{dose_per_kg_unit} ({patient_weight_kg:.1f}kg)"
                    else:
                        # Fixed dose
                        dose_mL = float(event.get('dose_mL', 5))
                        if conc_unit == 'mg/mL':
                            total_mass_mg = dose_mL * concentration
                        elif conc_unit == 'ug/mL':
                            total_mass_mg = dose_mL * concentration / 1000.0
                        elif conc_unit == 'g/L':
                            total_mass_mg = dose_mL * concentration
                        else:
                            total_mass_mg = dose_mL * concentration
                        dose_desc = f"{total_mass_mg:.1f}mg"

                    # Convert concentration to g/L for Pulse
                    if conc_unit == 'mg/mL':
                        conc_g_L = concentration
                    elif conc_unit == 'ug/mL':
                        conc_g_L = concentration / 1000.0
                    elif conc_unit == 'g/L':
                        conc_g_L = concentration
                    else:
                        conc_g_L = concentration

                    try:
                        infusion = SESubstanceInfusion()
                        infusion.set_comment(f"Bolus delivery of {drug}")
                        infusion.set_substance(drug)
                        infusion.get_rate().set_value(dose_mL * 60.0, VolumePerTimeUnit.mL_Per_min)
                        infusion.get_concentration().set_value(conc_g_L, MassPerVolumeUnit.from_string("g/L"))
                        pulse.process_action(infusion)
                        event_name = f"Bolus({drug},{dose_desc})"
                    except Exception as e:
                        event_name = f"Bolus FAILED ({e})"
                
                elif etype == 'infusion':
                    drug = event.get('drug', 'Norepinephrine')
                    conc = event.get('concentration', 0.016)
                    rate = event.get('rate_ml_per_hr', 10)
                    substance = pulse.get_substance_manager().get_substance(drug)
                    if substance:
                        infusion = SESubstanceInfusion()
                        infusion.set_substance(substance)
                        infusion.get_concentration().set_value(conc, MassPerVolumeUnit.from_string("mg/mL"))
                        infusion.get_rate().set_value(rate / 60.0, VolumePerTimeUnit.mL_Per_min)
                        pulse.process_action(infusion)
                        event_name = f"Infusion({drug},{rate}mL/hr)"
                
                elif etype == 'compound_infusion':
                    compound = event.get('compound', 'Saline')
                    rate = event.get('rate_mL_per_min', event.get('rate_ml_per_min', 100))
                    bag_volume = event.get('bag_volume_mL', 1000)
                    try:
                        infusion = SESubstanceCompoundInfusion()
                        infusion.set_compound(compound)  # Pass string directly, not compound object
                        infusion.get_rate().set_value(rate, VolumePerTimeUnit.mL_Per_min)
                        infusion.get_bag_volume().set_value(bag_volume, VolumeUnit.mL)
                        pulse.process_action(infusion)
                        event_name = f"Compound({compound},{bag_volume}mL@{rate}mL/min)"
                    except Exception as e:
                        print(f"[Batch] Compound infusion error: {e}")

                elif etype == 'start_controller':
                    controller_name = event.get('controller', 'default_controller')

                    if controller_name == 'http_controller':
                        url = event.get('http_url', 'http://localhost:5001')
                        config = event.get('http_config', {})
                        timeout = event.get('http_timeout', 10.0)

                        # Build simulation context for concurrent batch identification
                        batch_id_for_controller = batch_config.get('batch_id', '')
                        simulation_context = {
                            'simulation_id': f"{batch_id_for_controller}_{job_id}" if batch_id_for_controller else job_id,
                            'batch_id': batch_id_for_controller,
                            'job_id': job_id
                        }

                        try:
                            controller = BatchHTTPController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                            controller.send_init(patient_name, vent_settings)
                            controller_active = True
                            control_interval_s = controller.next_update_s
                            event_name = f"HTTPController({url})"
                        except Exception as e:
                            print(f"[Batch] HTTP Controller init failed: {e}")
                            event_name = f"HTTPController FAILED ({e})"
                    else:
                        # Built-in controller
                        controller = BatchBuiltinController(controller_name)
                        controller.send_init(patient_name, vent_settings)
                        controller_active = True
                        event_name = f"Controller({controller_name})"

                elif etype == 'stop_controller':
                    if controller_active:
                        controller_active = False
                        if controller:
                            controller.shutdown()
                        event_name = "Controller stopped"

                elif etype == 'start_fluid_controller':
                    fluid_controller_name = event.get('controller', 'default_fluid_controller')

                    if fluid_controller_name == 'http_fluid_controller':
                        # HTTP fluid controller
                        url = event.get('http_url', 'http://localhost:5001/fluid')
                        config = event.get('config', {})
                        timeout = event.get('timeout', 5.0)
                        batch_id_for_controller = batch_config.get('batch_id', '')
                        simulation_context = {
                            'simulation_id': f"{batch_id_for_controller}_{job_id}" if batch_id_for_controller else job_id,
                            'batch_id': batch_id_for_controller,
                            'job_id': job_id
                        }
                        try:
                            fluid_controller = BatchHTTPFluidController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                            fluid_controller.send_init(patient_name, fluid_settings)
                            fluid_controller_active = True
                            fluid_control_interval_s = 10.0
                            event_name = f"HTTPFluidController({url})"
                        except Exception as e:
                            print(f"[Batch] HTTP Fluid Controller init failed: {e}")
                            event_name = f"HTTPFluidController FAILED ({e})"
                    else:
                        # Built-in fluid controller
                        fluid_controller = BatchBuiltinFluidController(fluid_controller_name)
                        fluid_controller.send_init(patient_name, fluid_settings)
                        fluid_controller_active = True
                        fluid_control_interval_s = 10.0
                        event_name = f"FluidController({fluid_controller_name})"

                elif etype == 'stop_fluid_controller':
                    if fluid_controller_active:
                        fluid_controller_active = False
                        # Stop any active infusions
                        fluid_settings['crystalloid_rate_ml_min'] = 0
                        fluid_settings['blood_rate_ml_min'] = 0
                        # Apply zero rates to stop infusions
                        for compound in ['Saline', 'Blood']:
                            try:
                                infusion = SESubstanceCompoundInfusion()
                                infusion.set_compound(compound)
                                infusion.get_rate().set_value(0, VolumePerTimeUnit.mL_Per_min)
                                infusion.get_bag_volume().set_value(0, VolumeUnit.mL)
                                pulse.process_action(infusion)
                            except:
                                pass
                        active_infusions.clear()
                        event_name = "FluidController stopped"

                if event_name:
                    current_events.append(event_name)
                    # Track event type for after_event triggers
                    if etype:
                        fired_event_names.add(etype)
                event_idx += 1

            # Join multiple events with semicolon
            event_annotation = "; ".join(current_events) if current_events else ""
            
            # Advance time
            pulse.advance_time_s(timestep)
            sim_time += timestep
            
            # Pull data
            results = pulse.pull_data()

            # Build vitals dict dynamically from selected variables
            batch_vitals = {"t": results[0]}
            for vi, var in enumerate(selected_vars):
                val = results[vi + 1]
                transform = var.get("transform")
                if transform == "multiply_100":
                    val = val * 100
                elif transform == "lactate_g_to_mmol":
                    val = val * 11.1
                batch_vitals[var["key"]] = val

            # Controller step - run PCLC if active
            controller_cmd = ""
            if controller_active and controller and (sim_time - last_control_time) >= control_interval_s:
                # Build data payload for controller
                controller_data = {"sim_time_s": sim_time}
                controller_data.update(batch_vitals)

                response = controller.step(controller_data, vent_settings)

                if response:
                    changes = []
                    for key in ['mode', 'vt_ml', 'rr', 'fio2', 'peep_cmh2o', 'pinsp_cmh2o', 'itime_s']:
                        if key in response:
                            old_val = vent_settings.get(key)
                            new_val = response[key]
                            if old_val != new_val:
                                if key == 'fio2':
                                    changes.append(f"FiO2:{old_val:.0%}->{new_val:.0%}")
                                elif key == 'mode':
                                    changes.append(f"Mode:{new_val}")
                                else:
                                    key_short = key.replace('_cmh2o', '').replace('_ml', '').replace('_s', '')
                                    changes.append(f"{key_short}:{old_val}->{new_val}")
                            vent_settings[key] = new_val

                    if changes:
                        controller_cmd = "; ".join(changes)

                    # Apply ventilator changes if vent is active
                    if vent_active:
                        vent = SEMechanicalVentilatorVolumeControl()
                        vent.set_connection(eSwitch.On)
                        vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
                        vent.get_tidal_volume().set_value(vent_settings.get('vt_ml', 420), VolumeUnit.mL)
                        vent.get_respiration_rate().set_value(vent_settings.get('rr', 14), FrequencyUnit.Per_min)
                        vent.get_fraction_inspired_oxygen().set_value(vent_settings.get('fio2', 0.4))
                        vent.get_positive_end_expired_pressure().set_value(vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
                        vent.get_inspiratory_period().set_value(vent_settings.get('itime_s', 1.0), TimeUnit.s)
                        vent.get_flow().set_value(vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
                        pulse.process_action(vent)

                    # Update control interval from response
                    if 'next_interval_s' in response:
                        control_interval_s = max(0.1, response['next_interval_s'])

                last_control_time = sim_time

            # Fluid controller step - run if active
            fluid_controller_cmd = ""
            if fluid_controller_active and fluid_controller and (sim_time - last_fluid_control_time) >= fluid_control_interval_s:
                # Build vitals for fluid controller - HTTP controllers need sim_time
                if isinstance(fluid_controller, BatchHTTPFluidController):
                    fluid_vitals = {
                        "sim_time_s": sim_time,
                        "hr_bpm": batch_vitals.get("hr_bpm", 0),
                        "map_mmhg": batch_vitals.get("map_mmhg", 0),
                        "sbp_mmhg": batch_vitals.get("sbp_mmhg", 0),
                        "dbp_mmhg": batch_vitals.get("dbp_mmhg", 0),
                        "spo2_pct": batch_vitals.get("spo2_pct", 0),
                        "lactate_mmol_l": batch_vitals.get("lactate_mmol_l", 0),
                    }
                else:
                    fluid_vitals = batch_vitals

                fluid_response = fluid_controller.step(
                    fluid_vitals,
                    fluid_settings,
                    blood_loss_ml=cumulative_blood_loss_ml,
                    blood_infused_ml=cumulative_blood_infused_ml,
                    crystalloid_infused_ml=cumulative_crystalloid_infused_ml
                )

                if fluid_response:
                    changes = []

                    # Helper to check cancellation before potentially-blocking Pulse calls
                    def check_cancel_before_pulse():
                        if batch_id:
                            temp_dir = os.path.realpath(tempfile.gettempdir())
                            cancel_flag_path = os.path.join(temp_dir, f"pulse_batch_cancel_{batch_id}.flag")
                            return os.path.exists(cancel_flag_path)
                        return False

                    # Quick cancel check before applying infusions (which may block)
                    if check_cancel_before_pulse():
                        was_cancelled = True
                        break

                    # Apply crystalloid rate changes
                    new_crystalloid_rate = fluid_response.get('crystalloid_rate_ml_min', 0)
                    old_crystalloid_rate = fluid_settings.get('crystalloid_rate_ml_min', 0)
                    if new_crystalloid_rate != old_crystalloid_rate:
                        changes.append(f"Cryst:{old_crystalloid_rate}->{new_crystalloid_rate}mL/min")
                        # Apply crystalloid infusion
                        compound = fluid_settings.get('crystalloid_compound', 'Saline')
                        try:
                            infusion = SESubstanceCompoundInfusion()
                            infusion.set_compound(compound)
                            infusion.get_rate().set_value(new_crystalloid_rate, VolumePerTimeUnit.mL_Per_min)
                            infusion.get_bag_volume().set_value(10000, VolumeUnit.mL)
                            pulse.process_action(infusion)
                            if new_crystalloid_rate > 0:
                                active_infusions[compound] = {'rate': new_crystalloid_rate, 'remaining_ml': 10000, 'is_blood': False}
                            elif compound in active_infusions:
                                del active_infusions[compound]
                        except Exception as e:
                            print(f"[Batch] Crystalloid infusion error: {e}")

                    # Cancel check between infusions (before blood which may hang)
                    if check_cancel_before_pulse():
                        was_cancelled = True
                        break

                    # Apply blood rate changes
                    new_blood_rate = fluid_response.get('blood_rate_ml_min', 0)
                    old_blood_rate = fluid_settings.get('blood_rate_ml_min', 0)
                    if new_blood_rate != old_blood_rate:
                        changes.append(f"Blood:{old_blood_rate}->{new_blood_rate}mL/min")
                        # Apply blood infusion
                        compound = fluid_settings.get('blood_compound', 'Blood')
                        try:
                            infusion = SESubstanceCompoundInfusion()
                            infusion.set_compound(compound)
                            infusion.get_rate().set_value(new_blood_rate, VolumePerTimeUnit.mL_Per_min)
                            infusion.get_bag_volume().set_value(10000, VolumeUnit.mL)
                            pulse.process_action(infusion)
                            if new_blood_rate > 0:
                                active_infusions[compound] = {'rate': new_blood_rate, 'remaining_ml': 10000, 'is_blood': True}
                            elif compound in active_infusions:
                                del active_infusions[compound]
                        except Exception as e:
                            print(f"[Batch] Blood infusion error: {e}")

                    if changes:
                        fluid_controller_cmd = "; ".join(changes)

                    fluid_settings.update(fluid_response)
                    if 'next_interval_s' in fluid_response:
                        fluid_control_interval_s = max(1.0, fluid_response['next_interval_s'])

                last_fluid_control_time = sim_time

            # Blood loss tracking (from hemorrhage)
            total_flow_rate = sum(hemorrhage_flow_rates.values())
            blood_loss_this_step = total_flow_rate * (timestep / 60.0)
            cumulative_blood_loss_ml += blood_loss_this_step

            # Infusion tracking (for cumulative totals)
            for compound, info in list(active_infusions.items()):
                if info['remaining_ml'] > 0:
                    volume_this_step = info['rate'] * (timestep / 60.0)
                    actual_volume = min(volume_this_step, info['remaining_ml'])
                    info['remaining_ml'] -= actual_volume

                    if info['is_blood']:
                        cumulative_blood_infused_ml += actual_volume
                    else:
                        cumulative_crystalloid_infused_ml += actual_volume

                    if info['remaining_ml'] <= 0:
                        del active_infusions[compound]

            # Check conditional hemorrhage stops
            compartments_to_stop = []
            for compartment, condition in hemorrhage_stop_conditions.items():
                vital_name = condition["vital"]
                operator = condition["operator"]
                threshold = condition["value"]
                max_duration = condition["maxDurationSec"]
                start_time = condition["startTime"]

                current_value = batch_vitals.get(vital_name, 0)
                condition_met = False
                if operator == ">=" and current_value >= threshold:
                    condition_met = True
                elif operator == "<=" and current_value <= threshold:
                    condition_met = True
                elif operator == ">" and current_value > threshold:
                    condition_met = True
                elif operator == "<" and current_value < threshold:
                    condition_met = True

                # Also check max duration
                elapsed = sim_time - start_time
                if elapsed >= max_duration:
                    condition_met = True

                if condition_met:
                    compartments_to_stop.append((compartment, f"{vital_name} {operator} {threshold}"))

            # Stop hemorrhages that met their conditions
            for compartment, reason in compartments_to_stop:
                compartment_map = {
                    "RightLeg": eHemorrhage_Compartment.RightLeg,
                    "LeftLeg": eHemorrhage_Compartment.LeftLeg,
                    "RightArm": eHemorrhage_Compartment.RightArm,
                    "LeftArm": eHemorrhage_Compartment.LeftArm,
                    "Aorta": eHemorrhage_Compartment.Aorta,
                    "VenaCava": eHemorrhage_Compartment.VenaCava,
                }
                action = SEHemorrhage()
                action.set_compartment(compartment_map.get(compartment, eHemorrhage_Compartment.VenaCava))
                action.get_flow_rate().set_value(0, VolumePerTimeUnit.mL_Per_min)
                pulse.process_action(action)
                del hemorrhage_stop_conditions[compartment]
                stop_annotation = f"StopHemorrhage({compartment}, {reason})"
                if event_annotation:
                    event_annotation += "; " + stop_annotation
                else:
                    event_annotation = stop_annotation

            # Check triggered events (condition-based interventions)
            # First, check for any pending delayed triggers that are ready to fire
            events_ready_from_delay = []
            for event_id, fire_time in list(pending_delayed_triggers.items()):
                if sim_time >= fire_time:
                    # Find the event with this id
                    for event in triggered_events:
                        eid = event.get("id", id(event))
                        if eid == event_id:
                            events_ready_from_delay.append(event)
                            fired_trigger_ids.add(event_id)
                            break
                    del pending_delayed_triggers[event_id]

            # Now check all triggered events for new condition matches
            for event in triggered_events:
                event_id = event.get("id", id(event))

                # Skip if already fired or already pending delay
                if event_id in fired_trigger_ids:
                    continue
                if event_id in pending_delayed_triggers:
                    continue

                trigger = event.get("trigger")
                if not trigger:
                    continue

                condition_met = False

                # Check for after_event trigger (event-based)
                after_event = trigger.get("after_event")
                if after_event:
                    if after_event in fired_event_names:
                        condition_met = True
                else:
                    # Vital-based trigger
                    vital_name = trigger.get("vital")
                    operator = trigger.get("operator")
                    threshold = trigger.get("value")

                    if vital_name and operator and threshold is not None:
                        current_value = batch_vitals.get(vital_name, 0)
                        if operator == ">=" and current_value >= threshold:
                            condition_met = True
                        elif operator == "<=" and current_value <= threshold:
                            condition_met = True
                        elif operator == ">" and current_value > threshold:
                            condition_met = True
                        elif operator == "<" and current_value < threshold:
                            condition_met = True

                if condition_met:
                    delay_s = trigger.get("delay_s", 0)
                    if delay_s > 0:
                        # Schedule for later firing
                        pending_delayed_triggers[event_id] = sim_time + delay_s
                    else:
                        # Fire immediately
                        events_ready_from_delay.append(event)
                        fired_trigger_ids.add(event_id)

            # Process all events that are ready to fire (from delay or immediate)
            for event in events_ready_from_delay:
                event_id = event.get("id", id(event))
                trigger = event.get("trigger", {})

                # Build trigger description based on trigger type
                after_event = trigger.get("after_event")
                delay_s = trigger.get("delay_s", 0)
                if after_event:
                    trigger_desc = f"after:{after_event}"
                    if delay_s > 0:
                        trigger_desc += f"+{delay_s}s"
                else:
                    vital_name = trigger.get("vital")
                    operator = trigger.get("operator")
                    threshold = trigger.get("value")
                    trigger_desc = f"{vital_name} {operator} {threshold}"
                    if delay_s > 0:
                        trigger_desc += f"+{delay_s}s delay"

                    # Process the triggered event - same logic as time-based events
                    etype = event.get('type', '')
                    triggered_event_name = ""

                    if etype == 'start_controller':
                        controller_name = event.get('controller', 'default_controller')
                        if controller_name == 'http_controller':
                            url = event.get('http_url', 'http://localhost:5001')
                            config = event.get('config', {})
                            timeout = event.get('timeout', 5.0)
                            batch_id_for_controller = batch_config.get('batch_id', '')
                            simulation_context = {
                                'simulation_id': f"{batch_id_for_controller}_{job_id}" if batch_id_for_controller else job_id,
                                'batch_id': batch_id_for_controller,
                                'job_id': job_id
                            }
                            try:
                                controller = BatchHTTPController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                                controller.send_init(patient_name, vent_settings)
                                controller_active = True
                                control_interval_s = 10.0
                                triggered_event_name = f"HTTPController({url})"
                            except Exception as e:
                                print(f"[Batch] HTTP Controller init failed: {e}")
                                triggered_event_name = f"HTTPController FAILED ({e})"
                        else:
                            controller = BatchBuiltinController(controller_name)
                            controller_active = True
                            control_interval_s = 10.0
                            triggered_event_name = f"Controller({controller_name})"

                    elif etype == 'start_fluid_controller':
                        fluid_controller_name = event.get('controller', 'default_fluid_controller')
                        if fluid_controller_name == 'http_fluid_controller':
                            url = event.get('http_url', 'http://localhost:5001/fluid')
                            config = event.get('config', {})
                            timeout = event.get('timeout', 5.0)
                            batch_id_for_controller = batch_config.get('batch_id', '')
                            simulation_context = {
                                'simulation_id': f"{batch_id_for_controller}_{job_id}" if batch_id_for_controller else job_id,
                                'batch_id': batch_id_for_controller,
                                'job_id': job_id
                            }
                            try:
                                fluid_controller = BatchHTTPFluidController(url, config=config, timeout=timeout, simulation_context=simulation_context)
                                fluid_controller.send_init(patient_name, fluid_settings)
                                fluid_controller_active = True
                                fluid_control_interval_s = 10.0
                                triggered_event_name = f"HTTPFluidController({url})"
                            except Exception as e:
                                print(f"[Batch] HTTP Fluid Controller init failed: {e}")
                                triggered_event_name = f"HTTPFluidController FAILED ({e})"
                        else:
                            fluid_controller = BatchBuiltinFluidController(fluid_controller_name)
                            fluid_controller.send_init(patient_name, fluid_settings)
                            fluid_controller_active = True
                            fluid_control_interval_s = 10.0
                            triggered_event_name = f"FluidController({fluid_controller_name})"

                    elif etype == 'stop_fluid_controller':
                        if fluid_controller_active:
                            fluid_controller_active = False
                            fluid_settings['crystalloid_rate_ml_min'] = 0
                            fluid_settings['blood_rate_ml_min'] = 0
                            for compound in ['Saline', 'Blood']:
                                try:
                                    infusion = SESubstanceCompoundInfusion()
                                    infusion.set_compound(compound)
                                    infusion.get_rate().set_value(0, VolumePerTimeUnit.mL_Per_min)
                                    infusion.get_bag_volume().set_value(0, VolumeUnit.mL)
                                    pulse.process_action(infusion)
                                except:
                                    pass
                            triggered_event_name = "StopFluidController"

                    elif etype == 'intubate':
                        intubation_type = event.get('intubationType', 'Tracheal')
                        type_map = {
                            'Tracheal': eIntubationType.Tracheal,
                            'RightMainstem': eIntubationType.RightMainstem,
                            'LeftMainstem': eIntubationType.LeftMainstem,
                            'Esophageal': eIntubationType.Esophageal,
                            'Oropharyngeal': eIntubationType.Oropharyngeal,
                            'Nasopharyngeal': eIntubationType.Nasopharyngeal,
                        }
                        intubation = SEIntubation()
                        intubation.set_type(type_map.get(intubation_type, eIntubationType.Tracheal))
                        pulse.process_action(intubation)
                        is_intubated = (intubation_type != 'Off')
                        triggered_event_name = f"Intubate({intubation_type})"

                    elif etype == 'start_vent':
                        evt_vent_settings = event.get('vent_settings', vent_settings)
                        vent = SEMechanicalVentilatorVolumeControl()
                        vent.set_connection(eSwitch.On)
                        vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
                        vent.get_tidal_volume().set_value(evt_vent_settings.get('vt_ml', 420), VolumeUnit.mL)
                        vent.get_respiration_rate().set_value(evt_vent_settings.get('rr', 14), FrequencyUnit.Per_min)
                        vent.get_fraction_inspired_oxygen().set_value(evt_vent_settings.get('fio2', 0.4))
                        vent.get_positive_end_expired_pressure().set_value(evt_vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
                        vent.get_inspiratory_period().set_value(evt_vent_settings.get('itime_s', 1.0), TimeUnit.s)
                        vent.get_flow().set_value(evt_vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
                        pulse.process_action(vent)
                        vent_active = True
                        vent_settings.update(evt_vent_settings)
                        triggered_event_name = "StartVent"

                    elif etype == 'change_vent':
                        evt_vent_settings = event.get('vent_settings', {})
                        if evt_vent_settings:
                            vent = SEMechanicalVentilatorVolumeControl()
                            vent.set_connection(eSwitch.On)
                            vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
                            vent.get_tidal_volume().set_value(evt_vent_settings.get('vt_ml', 420), VolumeUnit.mL)
                            vent.get_respiration_rate().set_value(evt_vent_settings.get('rr', 14), FrequencyUnit.Per_min)
                            vent.get_fraction_inspired_oxygen().set_value(evt_vent_settings.get('fio2', 0.4))
                            vent.get_positive_end_expired_pressure().set_value(evt_vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
                            vent.get_inspiratory_period().set_value(evt_vent_settings.get('itime_s', 1.0), TimeUnit.s)
                            vent.get_flow().set_value(evt_vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
                            pulse.process_action(vent)
                            vent_settings.update(evt_vent_settings)
                            triggered_event_name = "ChangeVent"

                    elif etype == 'bolus':
                        drug_name = event.get('drug', 'Rocuronium')
                        dose_mode = event.get('dose_mode', 'fixed')
                        concentration = float(event.get('concentration', 10))
                        conc_unit = event.get('concentration_unit', 'mg/mL')

                        # Calculate dose based on mode
                        if dose_mode == 'weight':
                            dose_per_kg = float(event.get('dose_per_kg', 0.5))
                            dose_per_kg_unit = event.get('dose_per_kg_unit', 'mg/kg')

                            if dose_per_kg_unit in ('ug/kg', 'mcg/kg'):
                                total_mass_mg = (dose_per_kg / 1000.0) * patient_weight_kg
                            else:
                                total_mass_mg = dose_per_kg * patient_weight_kg

                            if conc_unit == 'mg/mL':
                                conc_mg_per_mL = concentration
                            elif conc_unit == 'ug/mL':
                                conc_mg_per_mL = concentration / 1000.0
                            elif conc_unit == 'g/L':
                                conc_mg_per_mL = concentration
                            else:
                                conc_mg_per_mL = concentration

                            dose_mL = total_mass_mg / conc_mg_per_mL if conc_mg_per_mL > 0 else 1.0
                            dose_desc = f"{dose_per_kg}{dose_per_kg_unit} ({patient_weight_kg:.1f}kg)"
                        else:
                            dose_mL = float(event.get('dose_mL', 5))
                            if conc_unit == 'mg/mL':
                                total_mass_mg = dose_mL * concentration
                            elif conc_unit == 'ug/mL':
                                total_mass_mg = dose_mL * concentration / 1000.0
                            elif conc_unit == 'g/L':
                                total_mass_mg = dose_mL * concentration
                            else:
                                total_mass_mg = dose_mL * concentration
                            dose_desc = f"{total_mass_mg:.1f}mg"

                        if conc_unit == 'mg/mL':
                            conc_g_L = concentration
                        elif conc_unit == 'ug/mL':
                            conc_g_L = concentration / 1000.0
                        elif conc_unit == 'g/L':
                            conc_g_L = concentration
                        else:
                            conc_g_L = concentration

                        try:
                            infusion = SESubstanceInfusion()
                            infusion.set_comment(f"Bolus delivery of {drug_name}")
                            infusion.set_substance(drug_name)
                            infusion.get_rate().set_value(dose_mL * 60.0, VolumePerTimeUnit.mL_Per_min)
                            infusion.get_concentration().set_value(conc_g_L, MassPerVolumeUnit.from_string("g/L"))
                            pulse.process_action(infusion)
                            triggered_event_name = f"Bolus({drug_name},{dose_desc})"
                        except Exception as e:
                            triggered_event_name = f"Bolus FAILED ({e})"

                    elif etype == 'infusion':
                        drug_name = event.get('drug', 'Norepinephrine')
                        concentration = event.get('concentration', 0.004)
                        rate_ml_per_hr = event.get('rate_ml_per_hr', 10)
                        try:
                            infusion = SESubstanceInfusion()
                            infusion.set_substance(drug_name)
                            infusion.get_concentration().set_value(concentration, MassPerVolumeUnit.mg_Per_mL)
                            infusion.get_rate().set_value(rate_ml_per_hr, VolumePerTimeUnit.mL_Per_hr)
                            pulse.process_action(infusion)
                            triggered_event_name = f"Infusion({drug_name},{rate_ml_per_hr}mL/hr)"
                        except Exception as e:
                            triggered_event_name = f"Infusion FAILED ({e})"

                    elif etype == 'compound_infusion':
                        compound = event.get('compound', 'Saline')
                        rate_ml_per_min = event.get('rate_ml_per_min', 10)
                        try:
                            infusion = SESubstanceCompoundInfusion()
                            infusion.set_compound(compound)
                            infusion.get_rate().set_value(rate_ml_per_min, VolumePerTimeUnit.mL_Per_min)
                            infusion.get_bag_volume().set_value(500, VolumeUnit.mL)
                            pulse.process_action(infusion)
                            triggered_event_name = f"CompoundInfusion({compound},{rate_ml_per_min}mL/min)"
                        except Exception as e:
                            triggered_event_name = f"CompoundInfusion FAILED ({e})"

                    else:
                        triggered_event_name = f"{etype}"

                    # Build the full triggered annotation
                    trigger_annotation = f"TRIGGERED[{trigger_desc}]: {triggered_event_name}"
                    if event_annotation:
                        event_annotation += "; " + trigger_annotation
                    else:
                        event_annotation = trigger_annotation

                    # Track event type for after_event triggers (enables chaining)
                    if etype:
                        fired_event_names.add(etype)

            # Check for patient death via event handler
            if event_handler.is_dead and not patient_dead:
                patient_dead = True
                death_msg = f"PATIENT_DEATH ({event_handler.death_cause})"
                if event_annotation:
                    event_annotation += "; " + death_msg
                else:
                    event_annotation = death_msg

            # Fallback: also check vitals for death (HR=0, MAP<10, or CO=0)
            if not patient_dead:
                hr = batch_vitals.get("hr_bpm", 60)
                map_val = batch_vitals.get("map_mmhg", 70)
                co = batch_vitals.get("co_lpm", 5)
                if hr < 1 or map_val < 5 or co < 0.1:
                    patient_dead = True
                    if event_annotation:
                        event_annotation += "; PATIENT_DEATH (vitals)"
                    else:
                        event_annotation = "PATIENT_DEATH (vitals)"

            # Build CSV row dynamically
            row = [batch_vitals["t"]]
            row.extend([batch_vitals.get(v["key"], "") for v in selected_vars])
            row.extend([
                vent_settings.get('mode', 'VC-AC'),
                vent_settings.get('vt_ml', 420),
                vent_settings.get('rr', 14),
                vent_settings.get('fio2', 0.4),
                vent_settings.get('peep_cmh2o', 5),
                vent_settings.get('pinsp_cmh2o', ''),
                vent_settings.get('itime_s', 1.0),
                1 if is_intubated else 0,
                1 if vent_active else 0,
                1 if controller_active else 0,
                1 if fluid_controller_active else 0,
                round(cumulative_blood_loss_ml, 1),
                round(cumulative_blood_infused_ml, 1),
                round(cumulative_crystalloid_infused_ml, 1),
                fluid_settings.get('crystalloid_rate_ml_min', 0),
                fluid_settings.get('blood_rate_ml_min', 0),
                event_annotation,
                controller_cmd,
                fluid_controller_cmd
            ])
            writer.writerow(row)
        
        # Add summary note at end of CSV if patient died
        if patient_dead and event_handler:
            writer.writerow([])  # Empty row
            writer.writerow(["# SIMULATION SUMMARY"])
            if event_handler.is_dead:
                writer.writerow([f"# Patient Status: DECEASED - {event_handler.death_cause}"])
                writer.writerow([f"# Time of Death: {event_handler.death_time_s:.1f}s"])
            else:
                writer.writerow(["# Patient Status: DECEASED (vitals-based detection)"])
            if event_handler.event_log:
                writer.writerow(["# Critical Events:"])
                for time_s, event_name, active in event_handler.event_log:
                    status = "ONSET" if active else "RESOLVED"
                    writer.writerow([f"#   {time_s:.1f}s - {event_name} ({status})"])

        # Log end of simulation loop
        # Shutdown controllers if active
        if controller:
            controller.shutdown()
        if fluid_controller:
            fluid_controller.shutdown()

        # Save CSV - use safe name for file with replicate suffix
        safe_csv_name = patient_name.replace('@0s.json', '').replace(' ', '_')
        csv_path = os.path.join(output_dir, f"{safe_csv_name}{replicate_suffix}.csv")
        with open(csv_path, 'w', newline='') as f:
            f.write(csv_buffer.getvalue())

        pulse.clear()

        # Return cancelled status if we were cancelled
        if was_cancelled:
            return {
                'job_id': job_id,
                'patient': job_id,
                'patient_name': patient_name,
                'is_custom': is_custom,
                'status': 'cancelled',
                'csv_path': csv_path,  # Still provide partial CSV
                'duration': sim_time,  # Actual time completed
                'message': f'Cancelled at {sim_time:.1f}s of {duration_s}s'
            }

        return {
            'job_id': job_id,  # Unique ID including replicate suffix
            'patient': job_id,  # Legacy compatibility
            'patient_name': patient_name,
            'is_custom': is_custom,
            'status': 'complete',
            'csv_path': csv_path,
            'duration': duration_s
        }

    except Exception as e:
        import traceback
        return {
            'job_id': job_id,
            'patient': job_id,  # Legacy compatibility
            'patient_name': patient_name,
            'is_custom': is_custom,
            'status': 'error',
            'message': str(e),
            'traceback': traceback.format_exc()
        }


def run_batch_thread(batch_id, batch):
    """
    Run a batch of patients in parallel using multiprocessing.Pool.

    Uses imap_unordered for better progress visibility - results are
    yielded as each patient completes rather than waiting for all.

    Supports replicates: if replicates > 1, each patient is run multiple times
    with output files suffixed _r1, _r2, etc.
    """
    print(f"[BATCH] run_batch_thread started for {batch_id}")

    pre_stabilized = batch.get('patients', [])
    custom_patients = batch.get('custom_patients', [])  # List of {name, json}
    duration_s = batch.get('duration_s', 300)
    workers = batch.get('workers', max(1, (os.cpu_count() or 4) - 1))
    replicates = max(1, batch.get('replicates', 1))

    base_patient_count = len(pre_stabilized) + len(custom_patients)
    total_jobs = base_patient_count * replicates
    print(f"[BATCH] {batch_id}: {base_patient_count} patients x {replicates} replicates = {total_jobs} jobs")
    
    if base_patient_count == 0:
        with batch_lock:
            batches[batch_id]['status'] = 'error'
            batches[batch_id]['message'] = 'No patients selected'
        print(f"[BATCH] {batch_id}: No patients selected!")
        return

    # Cap workers at CPU count and total job count
    max_workers = os.cpu_count() or 4
    workers = min(workers, max_workers, total_jobs) if total_jobs > 0 else 1

    # Initialize job progress tracking (patient x replicate combinations)
    with batch_lock:
        batches[batch_id]['replicates'] = replicates
        batches[batch_id]['base_patient_count'] = base_patient_count
        batches[batch_id]['total_jobs'] = total_jobs

        # Pre-stabilized patients x replicates
        for p in pre_stabilized:
            for rep in range(1, replicates + 1):
                job_id = f"{p}_r{rep}" if replicates > 1 else p
                batches[batch_id]['patients'][job_id] = {
                    'status': 'queued',
                    'sim_time': 0,
                    'duration': duration_s,
                    'is_custom': False,
                    'replicate': rep,
                    'base_patient': p
                }
        # Custom patients x replicates
        for cp in custom_patients:
            for rep in range(1, replicates + 1):
                job_id = f"custom_{cp['name']}_r{rep}" if replicates > 1 else f"custom_{cp['name']}"
                batches[batch_id]['patients'][job_id] = {
                    'status': 'queued',
                    'sim_time': 0,
                    'duration': duration_s,
                    'is_custom': True,
                    'replicate': rep,
                    'base_patient': cp['name']
                }
        batches[batch_id]['workers'] = workers
    
    try:
        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_dir = os.path.join(RESULTS_FOLDER, f"batch_{batch_id}_{timestamp}")
        os.makedirs(batch_dir, exist_ok=True)
        
        # Prepare batch config (without patients list)
        # Include PULSE_BIN and PULSE_PYTHON so worker processes know where to chdir and import from
        # v6: start_intubated defaults to False - use events for intubation/ventilation
        batch_config = {
            'batch_id': batch_id,  # For HTTP controller concurrent identification
            'duration_s': duration_s,
            'sample_rate_hz': batch.get('sample_rate_hz', 50),
            'start_intubated': batch.get('start_intubated', False),
            'vent_settings': batch.get('vent_settings', {}),
            'events': batch.get('events', []),
            'pulse_bin': PULSE_BIN,
            'pulse_python': PULSE_PYTHON,
            'output_columns': batch.get('output_columns'),
            'available_variables': AVAILABLE_VARIABLES,
        }

        # Clear any stale cancel flag for this batch (file-based)
        clear_batch_cancel_flag(batch_id)

        # Create argument tuples for each patient x replicate combination
        # Pre-stabilized: pass filename string
        # Custom: pass dict with {name, json}
        # Include replicate info for file naming
        # Cancellation is checked via file-based flag, not passed in args
        patient_args = []
        for p in pre_stabilized:
            for rep in range(1, replicates + 1):
                job_id = f"{p}_r{rep}" if replicates > 1 else p
                config_with_rep = batch_config.copy()
                config_with_rep['replicate'] = rep
                config_with_rep['replicate_suffix'] = f"_r{rep}" if replicates > 1 else ""
                patient_args.append((p, config_with_rep, batch_dir, job_id))
        for cp in custom_patients:
            for rep in range(1, replicates + 1):
                job_id = f"custom_{cp['name']}_r{rep}" if replicates > 1 else f"custom_{cp['name']}"
                config_with_rep = batch_config.copy()
                config_with_rep['replicate'] = rep
                config_with_rep['replicate_suffix'] = f"_r{rep}" if replicates > 1 else ""
                patient_args.append((cp, config_with_rep, batch_dir, job_id))

        # Run with ProcessPool using apply_async for non-blocking cancellation support
        print(f"Starting batch {batch_id} with {workers} workers for {total_jobs} jobs "
              f"({base_patient_count} patients x {replicates} replicates)")

        csv_paths = {}
        completed = 0
        cancelled = False

        with ProcessPool(processes=workers) as pool:
            # Submit all jobs asynchronously
            async_results = []
            for args in patient_args:
                async_results.append(pool.apply_async(run_single_patient, (args,)))

            # Poll for results with periodic cancel checks
            pending_results = list(enumerate(async_results))
            cancel_grace_period_start = None
            CANCEL_GRACE_PERIOD_S = 15  # Give workers 15 seconds to save partial results

            while pending_results:
                # Check for cancellation every iteration (roughly every 0.5s)
                if batch_cancel_flags.get(batch_id):
                    if cancel_grace_period_start is None:
                        # First time noticing cancellation - start grace period
                        cancel_grace_period_start = time.time()
                        print(f"[BATCH] Cancellation requested for {batch_id}, waiting up to {CANCEL_GRACE_PERIOD_S}s for {len(pending_results)} jobs to save partial results...")
                    elif time.time() - cancel_grace_period_start > CANCEL_GRACE_PERIOD_S:
                        # Grace period expired - force terminate
                        print(f"[BATCH] Grace period expired, terminating {len(pending_results)} remaining jobs...")
                        pool.terminate()
                        cancelled = True
                        break

                # Check each pending result with a short timeout
                still_pending = []
                for idx, async_result in pending_results:
                    try:
                        # Try to get result with short timeout
                        result = async_result.get(timeout=0.1)

                        # Process the result
                        job_id = result['job_id']
                        status = result['status']

                        with batch_lock:
                            if job_id in batches[batch_id]['patients']:
                                batches[batch_id]['patients'][job_id]['status'] = status
                                if status == 'complete':
                                    batches[batch_id]['patients'][job_id]['csv_path'] = result['csv_path']
                                    batches[batch_id]['patients'][job_id]['sim_time'] = result['duration']
                                    csv_paths[job_id] = result['csv_path']
                                elif status == 'cancelled':
                                    batches[batch_id]['patients'][job_id]['message'] = result.get('message', 'Cancelled')
                                    if result.get('csv_path'):
                                        csv_paths[job_id] = result['csv_path']
                                    print(f"[BATCH] Job {job_id} saved partial results: {result.get('message', '')}")
                                else:
                                    batches[batch_id]['patients'][job_id]['message'] = result.get('message', 'Unknown error')
                                    print(f"[ERROR] Job {job_id} failed:")
                                    print(f"  Message: {result.get('message', 'Unknown')}")
                                    if 'traceback' in result:
                                        print(f"  Traceback:\n{result['traceback']}")

                        completed += 1
                        print(f"Batch {batch_id}: {result.get('patient_name', job_id)} {status} ({completed}/{total_jobs})")

                    except MPTimeoutError:
                        # Result not ready yet, keep it in pending list
                        still_pending.append((idx, async_result))
                    except Exception as e:
                        # Job raised an exception
                        completed += 1
                        import traceback
                        print(f"[ERROR] Job raised exception: {e}")
                        print(f"[ERROR] Traceback: {traceback.format_exc()}")

                pending_results = still_pending

                # If we're in grace period and all jobs have finished, we're done
                if cancel_grace_period_start is not None and not pending_results:
                    cancelled = True
                    print(f"[BATCH] All jobs saved partial results")
                    break

                # Small sleep to prevent busy-waiting
                if pending_results:
                    time.sleep(0.1)

        # Clean up cancel flags (both thread-level and file-based)
        if batch_id in batch_cancel_flags:
            del batch_cancel_flags[batch_id]
        clear_batch_cancel_flag(batch_id)

        # Create ZIP of all results (complete or partial from cancelled jobs)
        # Even if cancelled, include whatever we collected
        if csv_paths:
            zip_path = os.path.join(RESULTS_FOLDER, f"batch_{batch_id}_{timestamp}.zip")
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for job_id, csv_path in csv_paths.items():
                    zf.write(csv_path, os.path.basename(csv_path))

            with batch_lock:
                batches[batch_id]['status'] = 'cancelled' if cancelled else 'complete'
                batches[batch_id]['zip_path'] = zip_path
                batches[batch_id]['batch_dir'] = batch_dir
                batches[batch_id]['completed_count'] = len(csv_paths)
                batches[batch_id]['total_count'] = total_jobs

            status_msg = "cancelled" if cancelled else "complete"
            print(f"Batch {batch_id} {status_msg}: {len(csv_paths)}/{total_jobs} jobs have results")
        else:
            # No results at all (very early cancellation or all jobs failed)
            with batch_lock:
                batches[batch_id]['status'] = 'cancelled' if cancelled else 'failed'
                batches[batch_id]['completed_count'] = 0
                batches[batch_id]['total_count'] = total_jobs
            print(f"Batch {batch_id} {'cancelled' if cancelled else 'failed'}: no results collected")
    
    except Exception as e:
        import traceback
        with batch_lock:
            batches[batch_id]['status'] = 'error'
            batches[batch_id]['message'] = str(e)
            batches[batch_id]['traceback'] = traceback.format_exc()
        print(f"Batch {batch_id} failed: {e}")


@app.route('/api/submit_batch', methods=['POST'])
def submit_batch():
    batch = request.json
    batch_id = str(uuid.uuid4())[:8]
    
    print(f"[BATCH] Received batch submission: {batch_id}")
    print(f"[BATCH] Patients: {batch.get('patients', [])}")
    print(f"[BATCH] Custom patients: {len(batch.get('custom_patients', []))}")
    
    with batch_lock:
        batches[batch_id] = {
            'status': 'running',
            'batch': batch,
            'patients': {},
            'started': datetime.now().isoformat()
        }
    
    thread = threading.Thread(target=run_batch_thread, args=(batch_id, batch), daemon=True)
    thread.start()
    
    print(f"[BATCH] Thread started for {batch_id}")
    return jsonify({'batch_id': batch_id})


@app.route('/api/batch_status/<batch_id>')
def batch_status(batch_id):
    with batch_lock:
        if batch_id not in batches:
            return jsonify({'status': 'not_found'}), 404
        return jsonify(batches[batch_id])


@app.route('/api/cancel_batch/<batch_id>', methods=['POST'])
def cancel_batch(batch_id):
    """Cancel a running batch."""
    with batch_lock:
        if batch_id not in batches:
            return jsonify({'success': False, 'error': 'Batch not found'}), 404

        status = batches[batch_id].get('status')
        if status != 'running':
            return jsonify({'success': False, 'error': f'Batch is {status}, cannot cancel'})

        # Set both the thread-level flag and the file-based cross-process flag
        batch_cancel_flags[batch_id] = True
        set_batch_cancel_flag(batch_id)  # File-based for worker processes

        batches[batch_id]['status'] = 'cancelling'
        return jsonify({'success': True, 'message': 'Cancellation requested'})


@app.route('/api/download_batch/<batch_id>')
def download_batch(batch_id):
    with batch_lock:
        if batch_id not in batches:
            return "Not found", 404
        batch_info = batches[batch_id]

    if 'zip_path' not in batch_info:
        return "Not ready", 400

    return send_file(batch_info['zip_path'], as_attachment=True)


# === VENT MECHANICS MODE ===
vm_jobs = {}
vm_lock = threading.Lock()


@app.route('/api/submit_vent_mech', methods=['POST'])
def submit_vent_mech():
    job = request.json
    job_id = str(uuid.uuid4())[:8]
    
    with vm_lock:
        vm_jobs[job_id] = {
            'status': 'running',
            'job': job,
            'sim_time': 0,
            'duration': job.get('duration_s', 60),
            'samples': 0,
            'started': datetime.now().isoformat()
        }
    
    thread = threading.Thread(target=run_vent_mech_thread, args=(job_id, job))
    thread.daemon = True
    thread.start()
    
    return jsonify({'job_id': job_id})


@app.route('/api/vm_status/<job_id>')
def vm_status(job_id):
    with vm_lock:
        if job_id not in vm_jobs:
            return jsonify({'status': 'not_found'}), 404
        return jsonify(vm_jobs[job_id])


@app.route('/api/download_vm/<job_id>')
def download_vm(job_id):
    with vm_lock:
        if job_id not in vm_jobs:
            return "Not found", 404
        job_info = vm_jobs[job_id]
    
    if 'csv_path' not in job_info:
        return "Not ready", 400
    
    return send_file(job_info['csv_path'], as_attachment=True)


def run_vent_mech_thread(job_id, job):
    """Run vent mechanics simulation in background thread."""
    try:
        os.chdir(PULSE_BIN)
        
        duration_s = job.get('duration_s', 60)
        sample_rate_hz = job.get('sample_rate_hz', 50)
        mechanics = job.get('mechanics', {})
        vent_settings = job.get('vent_settings', {})
        
        timestep = 1.0 / sample_rate_hz
        
        # Create vent mechanics engine (instant startup!)
        pulse = PulseEngine(eModelType.HumanAdultVentilationMechanics)
        pulse.set_log_filename(f"./test_results/vent_mech_{job_id}.log")
        pulse.log_to_console(False)
        
        # Data requests for vent mechanics
        data_requests = [
            # Physiology
            SEDataRequest.create_physiology_request("RespirationRate", unit=FrequencyUnit.Per_min),
            SEDataRequest.create_physiology_request("TidalVolume", unit=VolumeUnit.mL),
            SEDataRequest.create_physiology_request("TotalLungVolume", unit=VolumeUnit.mL),
            SEDataRequest.create_physiology_request("InspiratoryExpiratoryRatio"),
            SEDataRequest.create_physiology_request("MeanAirwayPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_physiology_request("TransrespiratoryPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_physiology_request("IntrapulmonaryPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_physiology_request("InspiratoryFlow", unit=VolumePerTimeUnit.L_Per_min),
            SEDataRequest.create_physiology_request("RespiratoryCompliance", VolumePerPressureUnit.mL_Per_cmH2O),
            SEDataRequest.create_physiology_request("TotalPulmonaryVentilation", VolumePerTimeUnit.mL_Per_min),
            # Ventilator
            SEDataRequest.create_mechanical_ventilator_request("AirwayPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("BreathState"),
            SEDataRequest.create_mechanical_ventilator_request("DynamicRespiratoryCompliance", unit=VolumePerPressureUnit.mL_Per_cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("ExpiratoryFlow", unit=VolumePerTimeUnit.L_Per_min),
            SEDataRequest.create_mechanical_ventilator_request("ExpiratoryTidalVolume", unit=VolumeUnit.mL),
            SEDataRequest.create_mechanical_ventilator_request("ExtrinsicPositiveEndExpiratoryPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("InspiratoryExpiratoryRatio"),
            SEDataRequest.create_mechanical_ventilator_request("InspiratoryFlow", unit=VolumePerTimeUnit.L_Per_min),
            SEDataRequest.create_mechanical_ventilator_request("InspiratoryTidalVolume", unit=VolumeUnit.mL),
            SEDataRequest.create_mechanical_ventilator_request("IntrinsicPositiveEndExpiratoryPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("MeanAirwayPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("PeakInspiratoryPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("PlateauPressure", unit=PressureUnit.cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("RespirationRate", unit=FrequencyUnit.Per_min),
            SEDataRequest.create_mechanical_ventilator_request("StaticRespiratoryCompliance", unit=VolumePerPressureUnit.mL_Per_cmH2O),
            SEDataRequest.create_mechanical_ventilator_request("TidalVolume", unit=VolumeUnit.mL),
            SEDataRequest.create_mechanical_ventilator_request("TotalPositiveEndExpiratoryPressure", unit=PressureUnit.cmH2O),
        ]
        data_mgr = SEDataRequestManager(data_requests)
        
        # Initialize with just sex (no patient file needed)
        pCfg = SEPatientConfiguration()
        pCfg.get_patient().set_sex(eSex.Male)
        pulse.initialize_engine(pCfg, data_mgr)
        
        # Configure respiratory mechanics
        config = SERespiratoryMechanicsConfiguration()
        mech = config.get_settings()
        mech.set_active(eSwitch.On)
        
        # Get mechanics parameters
        compliance = mechanics.get('compliance', 50.0)
        resistance = mechanics.get('resistance', 10.0)
        muscle_pressure = mechanics.get('muscle_pressure', 0.0)
        asymmetric = mechanics.get('asymmetric', False)
        
        # Calculate resistance distribution
        upper_resistance = resistance * 0.5
        side_resistance = resistance * 0.5
        
        # Calculate compliance
        if asymmetric:
            left_compliance = mechanics.get('left_compliance', compliance / 2)
            right_compliance = mechanics.get('right_compliance', compliance / 2)
            left_resistance = mechanics.get('left_resistance', resistance)
            right_resistance = mechanics.get('right_resistance', resistance)
        else:
            left_compliance = compliance / 2
            right_compliance = compliance / 2
            left_resistance = side_resistance
            right_resistance = side_resistance
        
        # Set resistances
        mech.get_upper_inspiratory_resistance().set_value(upper_resistance, PressureTimePerVolumeUnit.cmH2O_s_Per_L)
        mech.get_upper_expiratory_resistance().set_value(upper_resistance, PressureTimePerVolumeUnit.cmH2O_s_Per_L)
        mech.get_left_inspiratory_resistance().set_value(left_resistance, PressureTimePerVolumeUnit.cmH2O_s_Per_L)
        mech.get_left_expiratory_resistance().set_value(left_resistance, PressureTimePerVolumeUnit.cmH2O_s_Per_L)
        mech.get_right_inspiratory_resistance().set_value(right_resistance, PressureTimePerVolumeUnit.cmH2O_s_Per_L)
        mech.get_right_expiratory_resistance().set_value(right_resistance, PressureTimePerVolumeUnit.cmH2O_s_Per_L)
        
        # Set compliance curves (constant compliance)
        lcc = mech.get_left_compliance_curve()
        lc = lcc.add_constant_segment()
        lc.get_begin_volume().set_value(float("-inf"), VolumeUnit.mL)
        lc.get_end_volume().set_value(float("inf"), VolumeUnit.mL)
        lc.get_compliance().set_value(left_compliance, VolumePerPressureUnit.mL_Per_cmH2O)
        
        rcc = mech.get_right_compliance_curve()
        rc = rcc.add_constant_segment()
        rc.get_begin_volume().set_value(float("-inf"), VolumeUnit.mL)
        rc.get_end_volume().set_value(float("inf"), VolumeUnit.mL)
        rc.get_compliance().set_value(right_compliance, VolumePerPressureUnit.mL_Per_cmH2O)
        
        # Set breath timing (for passive patient, or patient effort)
        rr_bpm = vent_settings.get('rr', 14)
        itime_s = vent_settings.get('itime_s', 1.0)
        total_breath_time = 60.0 / rr_bpm
        insp_frac = itime_s / total_breath_time
        exp_frac = 1.0 - insp_frac
        
        mech.get_inspiratory_rise_time().set_value(insp_frac * total_breath_time, TimeUnit.s)
        mech.get_inspiratory_hold_time().set_value(0.0, TimeUnit.s)
        mech.get_inspiratory_release_time().set_value(min(insp_frac, exp_frac * 0.5) * total_breath_time, TimeUnit.s)
        mech.get_inspiratory_to_expiratory_pause_time().set_value(0.0, TimeUnit.s)
        mech.get_expiratory_rise_time().set_value(0.0, TimeUnit.s)
        mech.get_expiratory_hold_time().set_value(0.0, TimeUnit.s)
        mech.get_expiratory_release_time().set_value(0.0, TimeUnit.s)
        mech.get_inspiratory_peak_pressure().set_value(muscle_pressure, PressureUnit.cmH2O)
        mech.get_expiratory_peak_pressure().set_value(0.0, PressureUnit.cmH2O)
        
        pulse.process_action(config)
        
        # Intubate
        intubation = SEIntubation()
        intubation.set_type(eIntubationType.Tracheal)
        pulse.process_action(intubation)
        
        # Setup ventilator
        mode = vent_settings.get('mode', 'PC-AC')
        if mode.startswith('PC'):
            vent = SEMechanicalVentilatorPressureControl()
            vent.set_connection(eSwitch.On)
            vent.set_mode(eMechanicalVentilator_PressureControlMode.AssistedControl)
            vent.get_fraction_inspired_oxygen().set_value(vent_settings.get('fio2', 0.4))
            vent.get_inspiratory_period().set_value(vent_settings.get('itime_s', 1.0), TimeUnit.s)
            vent.get_inspiratory_pressure().set_value(vent_settings.get('pinsp_cmh2o', 15), PressureUnit.cmH2O)
            vent.get_positive_end_expired_pressure().set_value(vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
            vent.get_respiration_rate().set_value(vent_settings.get('rr', 14), FrequencyUnit.Per_min)
            vent.get_slope().set_value(vent_settings.get('rise_time_s', 0.2), TimeUnit.s)
        else:
            vent = SEMechanicalVentilatorVolumeControl()
            vent.set_connection(eSwitch.On)
            vent.set_mode(eMechanicalVentilator_VolumeControlMode.AssistedControl)
            vent.get_fraction_inspired_oxygen().set_value(vent_settings.get('fio2', 0.4))
            vent.get_inspiratory_period().set_value(vent_settings.get('itime_s', 1.0), TimeUnit.s)
            vent.get_tidal_volume().set_value(vent_settings.get('vt_ml', 420), VolumeUnit.mL)
            vent.get_positive_end_expired_pressure().set_value(vent_settings.get('peep_cmh2o', 5), PressureUnit.cmH2O)
            vent.get_respiration_rate().set_value(vent_settings.get('rr', 14), FrequencyUnit.Per_min)
            vent.get_flow().set_value(vent_settings.get('flow_lpm', 50), VolumePerTimeUnit.L_Per_min)
        
        pulse.process_action(vent)
        
        # Let settle briefly
        pulse.advance_time_s(2)
        
        # CSV output
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        csv_columns = [
            "sim_time_s", "rr_patient", "vt_ml", "total_lung_vol_ml", "ie_ratio",
            "mean_airway_prs_cmh2o", "trans_resp_prs_cmh2o", "intrapulm_prs_cmh2o",
            "insp_flow_lpm", "resp_compliance_ml_cmh2o", "minute_vent_ml_min",
            "vent_airway_prs_cmh2o", "breath_state", "dyn_compliance_ml_cmh2o",
            "vent_exp_flow_lpm", "exp_vt_ml", "peep_ext_cmh2o", "vent_ie_ratio",
            "vent_insp_flow_lpm", "insp_vt_ml", "peep_int_cmh2o", "vent_mean_paw_cmh2o",
            "pip_cmh2o", "pplat_cmh2o", "vent_rr", "static_compliance_ml_cmh2o",
            "vent_vt_ml", "total_peep_cmh2o"
        ]
        writer.writerow(csv_columns)
        
        # Run simulation
        sim_time = 0.0
        sample_count = 0
        
        while sim_time < duration_s:
            pulse.advance_time_s(timestep)
            sim_time += timestep
            
            results = pulse.pull_data()
            writer.writerow(results)
            sample_count += 1
            
            # Update progress
            with vm_lock:
                vm_jobs[job_id]['sim_time'] = sim_time
                vm_jobs[job_id]['samples'] = sample_count
        
        # Save CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(RESULTS_FOLDER, f"{job['name']}_{timestamp}.csv")
        with open(csv_path, 'w', newline='') as f:
            f.write(csv_buffer.getvalue())
        
        pulse.clear()
        
        with vm_lock:
            vm_jobs[job_id]['status'] = 'complete'
            vm_jobs[job_id]['csv_path'] = csv_path
            vm_jobs[job_id]['samples'] = sample_count
        
        print(f"[VM] Job {job_id} complete: {sample_count} samples")
        
    except Exception as e:
        import traceback
        with vm_lock:
            vm_jobs[job_id]['status'] = 'error'
            vm_jobs[job_id]['message'] = str(e)
            vm_jobs[job_id]['traceback'] = traceback.format_exc()
        print(f"[VM] Job {job_id} error: {e}")
        traceback.print_exc()



@app.route('/api/shutdown', methods=['POST'])
def shutdown():
    os._exit(0)


# === LIVE DATA STREAMING ===

# Available variables for live monitoring
LIVE_MONITOR_VARIABLES = [
    {'key': 'spo2_pct', 'label': 'SpO2 (%)', 'color': '#3498db', 'min': 70, 'max': 100},
    {'key': 'hr_bpm', 'label': 'Heart Rate (bpm)', 'color': '#e74c3c', 'min': 40, 'max': 180},
    {'key': 'map_mmhg', 'label': 'MAP (mmHg)', 'color': '#9b59b6', 'min': 40, 'max': 140},
    {'key': 'etco2_mmhg', 'label': 'EtCO2 (mmHg)', 'color': '#f39c12', 'min': 20, 'max': 60},
    {'key': 'rr_patient', 'label': 'RR Patient (/min)', 'color': '#1abc9c', 'min': 5, 'max': 40},
    {'key': 'pip_cmh2o', 'label': 'PIP (cmH2O)', 'color': '#e67e22', 'min': 0, 'max': 50},
    {'key': 'pao2_mmhg', 'label': 'PaO2 (mmHg)', 'color': '#2ecc71', 'min': 50, 'max': 500},
    {'key': 'paco2_mmhg', 'label': 'PaCO2 (mmHg)', 'color': '#8e44ad', 'min': 20, 'max': 80},
    {'key': 'ph', 'label': 'pH', 'color': '#16a085', 'min': 7.0, 'max': 7.6},
    {'key': 'co_lpm', 'label': 'Cardiac Output (L/min)', 'color': '#c0392b', 'min': 2, 'max': 10},
]


@app.route('/api/live_monitor_variables')
def live_monitor_variables():
    """Return list of available variables for live monitoring."""
    return jsonify(LIVE_MONITOR_VARIABLES)


@app.route('/api/subscribe_live/<job_id>', methods=['POST'])
def subscribe_live(job_id):
    """Subscribe to live data for a job."""
    data = request.json or {}
    variables = data.get('variables', ['spo2_pct', 'hr_bpm'])

    with live_data_lock:
        live_data_subscribers[job_id] = set(variables)

    return jsonify({'status': 'subscribed', 'variables': variables})


@app.route('/api/unsubscribe_live/<job_id>', methods=['POST'])
def unsubscribe_live(job_id):
    """Unsubscribe from live data for a job."""
    with live_data_lock:
        if job_id in live_data_subscribers:
            del live_data_subscribers[job_id]

    return jsonify({'status': 'unsubscribed'})


@socketio.on('connect')
def handle_connect():
    print(f"[WebSocket] Client connected")


@socketio.on('disconnect')
def handle_disconnect():
    print(f"[WebSocket] Client disconnected")


@socketio.on('subscribe')
def handle_subscribe(data):
    """Handle WebSocket subscription request."""
    job_id = data.get('job_id')
    variables = data.get('variables', ['spo2_pct', 'hr_bpm'])

    if job_id:
        with live_data_lock:
            live_data_subscribers[job_id] = set(variables)
        emit('subscribed', {'job_id': job_id, 'variables': variables})


@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    """Handle WebSocket unsubscription request."""
    job_id = data.get('job_id')

    if job_id:
        with live_data_lock:
            if job_id in live_data_subscribers:
                del live_data_subscribers[job_id]
        emit('unsubscribed', {'job_id': job_id})


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    
    parser = argparse.ArgumentParser(description='Pulse Simulation Server')
    parser.add_argument('--port', type=int, default=8080, help='Port to run on (default: 8080)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("  PULSE SIMULATION SERVER v6")
    print("="*60)
    print(f"  API running at http://{args.host}:{args.port}")
    print(f"  WebSocket enabled for live data streaming")
    print(f"  Pulse Home: {PULSE_HOME}")
    print(f"  CPUs: {os.cpu_count()}")
    print("="*60)
    print("  Connect with pulse_gui.py or any HTTP client")
    print("="*60 + "\n")

    socketio.run(app, host=args.host, port=args.port, debug=False, allow_unsafe_werkzeug=True)
