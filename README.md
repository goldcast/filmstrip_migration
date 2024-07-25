# filmstrip_migration
migration script for filmstrips

# To run this locally, do:

1. python3 -m venv venv   (create a virtual environment)     
2. . venv/bin/activate    (activate the virtual environment)
3. pip install -r requirements.txt  (install dependencies)


# Run migration for uploads for last 7 days

`python3 uploads.py --env='alpha' --days='7'`

# Run migration for url uploads for last 7 days

`python3 upload_urls.py --env='alpha' --days='7' --max_worker=5`

# Run migration for preseeded for last 7 days

`python3 upload_preseeded.py --env='alpha' --days='7'`

# Run migration for recordings for last 7 days

`python3 recordings.py --env='alpha' --days='7'`
