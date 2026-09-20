# TM Project Contributions Extractor

A Streamlit application for extracting and summarizing contribution history from a HOT Tasking Manager project.

## What it does

The app:

- accepts a Tasking Manager project ID and date range;
- retrieves project tasks and task history from the HOT Tasking Manager API;
- filters contribution events by date;
- summarizes contributions by user and action;
- reports mapped and validated task counts using the application's custom rules;
- provides tables, a contributor chart, and CSV downloads.

## Requirements

Python 3 and the packages listed in `requirements.txt`.

## Run

```bash
pip install -r requirements.txt
streamlit run app.py
```

Then enter a Tasking Manager project ID and date range in the web interface.

## Notes

The application calls the production HOT Tasking Manager API. API availability and project access can affect results.

## License

See [LICENSE](LICENSE).
