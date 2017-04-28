@echo off

for /f "delims=" %%i in ('type tickers.txt') do (
	get-data-0.5 -n -s "%%i" -sd 2016-05-30T00:00:00 -ed 2017-04-24T23:59:59
)