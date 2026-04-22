"""Entry-point shim — keeps `python dashboard.py [--port PORT] [--db PATH]` working.

Implementation lives in the dashboard/ package (data, components, pages,
app, lifecycle). NSSM service config and restart_dashboard.bat both
launch this file by path, so it must stay here even after the split.
"""

if __name__ == "__main__":
    from dashboard.lifecycle import main
    main()
