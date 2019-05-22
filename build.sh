#!/bin/bash

python3 sql_setup.py
python3 scores.py
python3 geo_link.py
python3 allocate_bikepods.py
python3 z_score.py

python3 correlation.py
python3 map_scores.py