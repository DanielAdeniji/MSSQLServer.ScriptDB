set "DIR_COMPILER=C:\WINDOWS\Microsoft.NET\Framework\v4.0.30319"
set "REF_DLL_LIST=output\Microsoft.SqlServer.Smo.dll, output\Microsoft.SqlServer.ConnectionInfo.dll"

set ConnectionInfo="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.ConnectionInfo.dll"
set Smo="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.Smo.dll"
set SdkSfc="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.Management.Sdk.Sfc.dll"
set SqlEnum="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.SqlEnum.dll"

set "APPNAME=bin\Debug\ScriptDB.exe"
set "APPCONFIG_BASE=app.config"
set "APPCONFIG=%APPNAME%.config"

If not exist "bin\Debug" md "bin\Debug"

if exist %APPNAME% del %APPNAME%

%DIR_COMPILER%\csc /reference:%ConnectionInfo%,%Smo%,%SdkSfc%,%SqlEnum% /define:DEBUG /optimize  /out:%APPNAME% *.cs

if exist %APPCONFIG_BASE% xcopy /Y /D %APPCONFIG_BASE% %APPCONFIG%