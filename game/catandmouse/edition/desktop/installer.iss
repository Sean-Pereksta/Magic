[Setup]
AppId={{B30EC1CA-4532-4FA9-B132-E474EC27055A}
AppName=Cat & Mouse Local Edition
AppVersion={#ReleaseVersion}
DefaultDirName={localappdata}\Programs\CatMouseLocal
DefaultGroupName=Cat & Mouse Local Edition
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=lowest
DisableProgramGroupPage=yes
OutputDir={#ReleaseDirectory}
OutputBaseFilename={#ReleaseName}-Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
UninstallDisplayName=Cat & Mouse Local Edition

[Files]
Source: "{#PayloadDirectory}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\Cat & Mouse Local Edition"; Filename: "{sys}\wscript.exe"; Parameters: """{app}\Play Cat Mouse.vbs"""; WorkingDir: "{app}"

[Run]
Filename: "{sys}\wscript.exe"; Parameters: """{app}\Play Cat Mouse.vbs"""; WorkingDir: "{app}"; Description: "Launch Cat & Mouse Local Edition"; Flags: nowait postinstall skipifsilent
