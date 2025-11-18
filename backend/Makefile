.PHONY: run clean

VENV = venv
PIP = $(VENV)/Scripts/pip 

$(VENV)/Scripts/activate: requirements.txt
	py -m venv $(VENV)
	$(PIP) install -r requirements.txt
	touch $(VENV)/bin/activate

venv: clean
	@echo "Creating a new virtual environment..."
	py -m venv venv $(VENV)
	$(PIP) install -r requirements.txt
	touch $(VENV)/Scripts/activate
	@echo "Virtual environment created."

run: $(VENV)/Scripts/activate
	uvicorn main:app --reload

clean:
	rm -rf __pycache__
	rm -rf $(VENV)
