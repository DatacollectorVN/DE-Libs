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

# 2. Setup ODBC Driver
## 2.1. For MacOS (M1)
- First install `Microsoft ODBC 17` (In my case, the ODBC 13 installed but it's not working). 
If your Azure SQL database recommend use `Driver={ODBC Driver 13 for SQL Server}` but in my case still use `Driver={ODBC Driver 17 for SQL Server}` and it's working.
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
echo YES | brew install msodbcsql17 mssql-tools
```

- Change path of ODBC to connecting with pyodbc
```
vim /opt/homebrew/etc/odbcinst.ini
``` 
Then change the path like this below
```
[ODBC Driver 17 for SQL Server]
Description=Microsoft ODBC Driver 13 for SQL Server
Driver=/opt/homebrew/Cellar/msodbcsql17/<version>/lib/libmsodbcsql.17.dylib
UsageCount=1
```

# 3. Test connection
- Change the metadata configuration your Azure SQL Database in `config/config_sample.ini` --> then change the name to `config.ini`.

- run file `test_connection.py` (press **F5**)