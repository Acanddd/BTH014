# Pickle Serialization Stability and Correctness Test
This repository contains test code for serializing and deserializing Python objects using the `pickle` module.

## Project Setup

1. **Clone the repository:**
   Make sure to clone this repository and all the `.py` files in the subdirectories.

   ```bash
   git clone https://github.com/Acanddd/BTH014

2. **Install dependencies:**
Install the necessary dependencies by running:
   ```bash
   pip install -r requirements.txt

## Running Tests
**Black-box testing with pytest:**

1.To run black-box tests using pytest, simply run the following command:

  ```bash
  pytest blackbox_pickle_test.py
```
**White-box testing with coverage:**

2.To run tests with coverage tracking, use:

  ```bash
  coverage run --source=pickle_1 -m pytest whitebox_pickle_test.py
  coverage report --include=pickle_1.py
```
