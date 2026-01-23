@echo off

echo Building SMB Backup...
go build -o .\smb-backup.exe .
go build -o .\build\smb-backup.exe .
if %errorlevel% equ 0 (
    echo Build successful!
    echo Output: .\build\smb-backup.exe
) else (
    echo Build failed!
)

echo [INFO] Building for linux/arm64...
set GOOS=linux
set GOARCH=arm64
go build -o .\build\smb-backup .
go build -o .\smb-backup .
if %errorlevel% equ 0 (
    echo [SUCCESS] linux/arm64 build completed
) else (
    echo [ERROR] linux/arm64 build failed with error code %errorlevel%
)

pause
