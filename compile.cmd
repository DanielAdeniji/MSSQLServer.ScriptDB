rem set DIR_COMPILER=C:\Windows\Microsoft.NET\Framework64\v4.0.30319
set "DIR_COMPILER=C:\WINDOWS\Microsoft.NET\Framework\v4.0.30319"

set "REF_DLL_LIST=output\Microsoft.SqlServer.Smo.dll, output\Microsoft.SqlServer.ConnectionInfo.dll"

set ConnectionInfo="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.ConnectionInfo.dll"
set Smo="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.Smo.dll"
set SdkSfc="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.Management.Sdk.Sfc.dll"
set SqlEnum="D:\Program Files\Microsoft SQL Server\120\SDK\Assemblies\Microsoft.SqlServer.SqlEnum.dll"

If not exist "output" md output

@rem DataLayer.cs(9,27): error CS0234: The type or namespace name 'Management' does    not exist in the namespace 'Microsoft.SqlServer' (are you missing an assembly reference?)
@rem %DIR_COMPILER%\csc /reference:%REF_DLL_LIST% /define:DEBUG /optimize  /out:SkeletonSetup.exe *.cs
rem %DIR_COMPILER%\csc /reference:output\Microsoft.SqlServer.ConnectionInfo.dll /define:DEBUG /optimize  /out:SkeletonSetup.exe *.cs

If not exist "bin" md bin
If not exist "bin\debug" md "bin\debug"

if exist bin\Debug\ScriptDB.exe del bin\Debug\ScriptDB.exe

%DIR_COMPILER%\csc /reference:%ConnectionInfo%,%Smo%,%SdkSfc%,%SqlEnum% /define:DEBUG /optimize  /out:bin\Debug\ScriptDB.exe *.cs

