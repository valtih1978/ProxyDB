@echo off

goto start

:test
:	"echo|set /p=%1 " >> timed
	echo timeit %1 >> timed
	call timeit %1 >> timed
	EXIT /B 1

:start

del /q C:\Users\valentin\AppData\Local\Temp\zdb_swap7797261140896837624\*
call scalac *.sc

del timed

set JAVA_HOME=c:\Program Files (x86)\Java\jdk1.8.0_31
call :test "scala -J-Xmx33m ProxyDemo + 16 100 > nul"
call :test "scala -J-Xmx33m ProxyDemo + 16 100 scacheoff > nul"
call :test "scala -J-Xmx1000m ProxyDemo + 18 1k > nul"
call :test "scala -J-Xmx1000m ProxyDemo + 18 1k scacheoff > nul"

set java_home=c:\Program Files\java\jre1.8.0_45
call :test "scala -J-Xmx33m ProxyDemo + 16 100 > nul"
call :test "scala -J-Xmx33m ProxyDemo + 16 100 scacheoff > nul"
call :test "scala -J-Xmx1000m ProxyDemo + 18 1k > nul"
call :test "scala -J-Xmx1000m ProxyDemo + 18 1k scacheoff > nul"


type timed && del timed