.\venv\Scripts\Activate.ps1
git stash save
git pull
pip install -r requirements.txt
./update_offers.py