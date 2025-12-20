# Data Quality Engine – Backend (DQE-X)

This repository contains the backend implementation of **DQE-X**, an automated
data quality scoring and preprocessing execution engine.

## Features
- CSV-only dataset upload
- Dataset quality scoring (0–100)
- Feature-level risk and leakage detection
- Automated preprocessing execution
- Undo / rollback support
- Before–after quality comparison
- Downloadable cleaned CSV

## Tech Stack
- Python 3.10
- FastAPI
- Uvicorn
- Pandas, NumPy
- Scikit-learn
- Statsmodels

## Setup Instructions

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
``` 