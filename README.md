# Trading Project

The python project requires `python>=3.10`

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd trading-project
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

3. Set up your Polygon.io API key:

   a. Get your API key from [Polygon.io](https://polygon.io)
   
   b. Set up the API key as an environment variable:

   **Windows PowerShell (temporary, for current session):**
   ```powershell
   $env:POLYGON_API_KEY="your-api-key-here"
   ```

   **Windows Command Prompt (temporary, for current session):**
   ```cmd
   set POLYGON_API_KEY=your-api-key-here
   ```

   **For permanent setup in Windows:**
   1. Open System Properties (Win + Pause/Break)
   2. Click on "Advanced system settings"
   3. Click on "Environment Variables"
   4. Under "User variables", click "New"
   5. Variable name: `POLYGON_API_KEY`
   6. Variable value: Your API key
   7. Click OK

   **Linux/macOS (temporary, for current session):**
   ```bash
   export POLYGON_API_KEY="your-api-key-here"
   ```

   **For permanent setup in Linux:**
   1. Open your shell's configuration file in a text editor:
   ```bash
   nano ~/.bashrc
   ```
   2. Add the following line at the end of the file:
   ```bash
   export POLYGON_API_KEY="your-api-key-here"
   ```
   3. Save the file and reload it:
   ```bash
   source ~/.bashrc
   ```

## Usage

Run the main script:
```bash
python main.py
```

## Project Structure

- `main.py`: Main entry point for the application
- `data_acquirer.py`: Handles fetching data from Polygon.io
- `data_processor.py`: Processes and analyzes the fetched data
- `config.py`: Configuration settings

## Data Storage

Processed data is stored in the `data/` directory in CSV format. This directory is not tracked in git.

## Security Note

Never expose your API key. The project is set up to use environment variables for secure API key management.