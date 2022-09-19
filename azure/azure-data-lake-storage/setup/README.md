# Data engineering libraries
# 1. How to install Azure library packages for Python
- Create python environment with conda
```bash
conda create -n DELibs-Azure python=3.10 -y 
conda activate DELibs-Azure
```

- Be sure you've added the Microsoft channel to your Conda configuration (you need to do this only once):
```bash
conda config --add channels "Microsoft"
```

- Install required packages:
```bash
pip install -r requirements.txt
```