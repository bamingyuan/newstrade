#!/usr/bin/env bash
set -e

cd /var/www/newstrade
source /var/www/newstrade/.venv/bin/activate
/var/www/newstrade/.venv/bin/newstrade run-all --window 1d --mode ibkr
