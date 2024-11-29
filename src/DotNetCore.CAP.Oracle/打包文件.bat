@echo OFF
color 0a
Title JGSoft-Nuget打包上传
:START
ECHO.
Echo                  ==========================================================================
ECHO.
Echo                                               JGSoft-Nuget打包上传
ECHO.
Echo                  ==========================================================================
Echo.
echo.
echo.
echo.
dotnet nuget push DotNetCore.CAP.Oracle.7.0.3.nupkg -k 9058B31A-D0E3-4C0A-9F4C-F4CE6BF021BA -s https://nuget.jgkld.jgzxsoft.com/nuget  --skip-duplicate
echo.
set /p xxf=xc:


