@echo off 

SETLOCAL
set workingEnv=ScalpFX
set workingPath=D:/

echo Opening Jupyter Notebook with path "%workingPath%" as its root working directory.
call conda activate %workingEnv%
echo:
echo.Conda Env: %workingEnv%
echo:
call jupyter notebook --notebook-dir %workingPath%

pause