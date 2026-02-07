# Pulse Scenario Builder

A web-based simulation environment for the [Pulse Physiology Engine](https://pulse.kitware.com/), enabling physiological simulations with support for mechanical ventilation, hemorrhage scenarios, and closed-loop controllers.

## Features

- **Single Patient Simulations**: Run individual patient scenarios with real-time vital sign monitoring
- **Batch Simulations**: Run multiple patients in parallel with automatic result collection
- **Ventilator Mechanics Mode**: Explore lung mechanics with configurable compliance and resistance
- **HTTP Controller Support**: Connect external physiological closed-loop controllers (PCLCs) for automated ventilator and fluid management
- **Live Data Streaming**: WebSocket-based real-time vital sign visualization
- **Scenario Builder**: GUI for creating complex scenarios with timed events and condition-based triggers

## Prerequisites

### 1. Pulse Physiology Engine

If you have built the Pulse physiology engine on your computer, then you should have a folder inside the build called 'install'.

Copy and paste that folder into the same directory as your pulse_server.py and pulse_gui.html files, and rename it 'pulse_engine'.

Your directory should look like this:

```
Pulse_Scenario_Builder/
├── pulse_engine/
│   ├── bin/          # Contains PulseC.dll and other binaries
│   ├── python/       # Python bindings
│   ├── include/
│   └── lib/
├── pulse_server.py
├── pulse_gui.html
└── README.md
```

### 2. Python Dependencies

Requires Python 3.8+ with the following packages:

```bash
pip install flask flask-cors flask-socketio requests
```

## Running the Server

Start the simulation server:

```bash
python pulse_server.py
```

You should see something like the following in the console:

```bash
============================================================
  PULSE SIMULATION SERVER v6
============================================================
  API running at http://0.0.0.0:8080
  WebSocket enabled for live data streaming
  Pulse Home: C:\path\to\your\build\of\pulse_engine
  CPUs: XX
============================================================
  Connect with pulse_gui.py or any HTTP client
============================================================

 * Serving Flask app 'pulse_server'
 * Debug mode: off
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:8080
 * Running on http://XXX.XXX.X.XX:8080
Press CTRL+C to quit
```

By default, the server runs on `http://0.0.0.0:8080`. You can customize the host and port:

```bash
python pulse_server.py --host 127.0.0.1 --port 5000
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Host address to bind to |
| `--port` | `8080` | Port number |

## Using the GUI

The GUI is loaded by the server. To access it:

1. Start the server as described above, and EITHER:
2.a. Open `pulse_gui.html` in a web browser; the GUI will automatically connect to `http://localhost:8080`
2.b. Open a web browser and point it to the IP address of the server - it should open the pulse_gui.html file

## Tabs Overview

### Single Patient
Run individual simulations with:
- Patient selection (pre-stabilized states or custom patient definitions)
- Event timeline (intubation, ventilation, hemorrhage, drug administration, etc.)
- Condition-based triggers (e.g., start controller when SpO2 drops below threshold)
- Real-time vital sign monitoring with live charts
- Optional HTTP controller integration

### Batch Simulation
Run multiple patients in parallel:
- Select multiple pre-stabilized patients
- Define custom patient variations
- Configure replicates for statistical runs
- Shared event timeline applied to all patients
- Results exported as ZIP file with CSV data for each patient

### Vent Mechanics
Explore ventilator and lung mechanics:
- Configure lung compliance and resistance
- Symmetric or asymmetric lung settings
- Visualize pressure-volume relationships

## HTTP Controllers

The system supports external HTTP-based physiological closed-loop controllers for:
- **Ventilator Control**: Automated ventilator adjustments
- **Fluid Resuscitation**: Automated crystalloid and blood product administration

Controllers communicate via REST API endpoints. See the `test_http_controller` endpoint for the expected interface.

## Output Data

Simulation results are saved as CSV files containing:
- Vital signs (HR, SpO2, blood pressure, respiratory parameters, etc.)
- Ventilator settings and measurements
- Blood gas values (PaO2, PaCO2, pH, lactate)
- Controller commands and events

## Project Structure

```
Pulse_Scenario_Builder/
├── pulse_server.py    # Flask server with Pulse Engine integration
├── pulse_gui.html     # Single-file web GUI (no build required)
├── pulse_engine/      # Pulse (not included, download/build separately)
├── results/           # Simulation output directory
└── uploads/           # Uploaded patient files
```

## License

This project interfaces with the Pulse Physiology Engine. See [Pulse License](https://pulse.kitware.com/) for engine licensing terms.

