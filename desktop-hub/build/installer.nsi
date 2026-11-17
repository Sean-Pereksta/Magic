Unicode true
!include "MUI2.nsh"
!include "LogicLib.nsh"
!include "FileFunc.nsh"
!include "x64.nsh"
RequestExecutionLevel user
Name "Catnmice Game Hub"
OutFile "${OUTDIR}\Catnmice-Game-Hub-Setup.exe"
InstallDir "$LOCALAPPDATA\Programs\Catnmice Game Hub"
SetCompressor /FINAL zlib
BrandingText "Catnmice.com"
Icon "${ICON}"
UninstallIcon "${ICON}"
VIProductVersion "${FILEVERSION}"
VIAddVersionKey ProductName "Catnmice Game Hub"
VIAddVersionKey ProductVersion "${VERSION}"
VIAddVersionKey FileVersion "${VERSION}"
VIAddVersionKey FileDescription "Catnmice Game Hub Setup"
VIAddVersionKey LegalCopyright "Catnmice"
!define MUI_ICON "${ICON}"
!define MUI_UNICON "${ICON}"
!define MUI_WELCOMEFINISHPAGE_BITMAP "${WIZARD}"
!define MUI_WELCOMEPAGE_TITLE "Catnmice Game Hub"
!define MUI_WELCOMEPAGE_TEXT "Install or repair your Catnmice library. Choose offline games on the next screen. Your profiles and game saves are stored separately."
!define MUI_COMPONENTSPAGE_TEXT_TOP "Choose the games to install or repair. The space required is shown below."
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"
Var OnePack
Var PortableTest
Function .onInit
  ${IfNot} ${RunningX64}
    MessageBox MB_ICONSTOP "Catnmice Game Hub requires 64-bit Windows."
    Abort
  ${EndIf}
  ${GetParameters} $0
  ${GetOptions} $0 "/PACK=" $OnePack
  ClearErrors
  ${GetOptions} $0 "/TESTROOT=" $PortableTest
  ClearErrors
  ${If} $PortableTest != ""
    StrCpy $INSTDIR "$PortableTest"
  ${EndIf}
FunctionEnd
Section "Catnmice Game Hub and Windows runtime" Core
  SectionIn RO
  ${If} $OnePack != ""
    IfFileExists "$INSTDIR\catnmice-install.txt" +2
      Abort "Install the launcher before adding a game."
    Goto core_end
  ${EndIf}
  SetOutPath "$INSTDIR"
  SetOverwrite try
  IfFileExists "$INSTDIR\Catnmice Game Hub.exe" 0 unlocked
    ClearErrors
    FileOpen $0 "$INSTDIR\Catnmice Game Hub.exe" a
    IfErrors locked
    FileClose $0
    Goto unlocked
    locked:
    MessageBox MB_ICONEXCLAMATION|MB_OK "Close Catnmice Game Hub, then run Setup again." /SD IDOK
    Abort
  unlocked:
  SetOverwrite on
  ClearErrors
  File /r "${APPDIR}\*.*"
  IfErrors 0 +2
    Abort "Setup could not install the launcher files."
  FileOpen $0 "$INSTDIR\catnmice-install.txt" w
  FileWrite $0 "Catnmice Game Hub"
  FileClose $0
  FileOpen $0 "$INSTDIR\setup-source.txt" w
  FileWriteUTF16LE /BOM $0 "$EXEPATH"
  FileClose $0
  WriteUninstaller "$INSTDIR\Uninstall.exe"
  ${If} $PortableTest == ""
    CreateDirectory "$SMPROGRAMS\Catnmice Game Hub"
    CreateShortcut "$SMPROGRAMS\Catnmice Game Hub\Catnmice Game Hub.lnk" "$INSTDIR\Catnmice Game Hub.exe"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "DisplayName" "Catnmice Game Hub"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "DisplayVersion" "${VERSION}"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "Publisher" "Catnmice"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "DisplayIcon" "$INSTDIR\Catnmice Game Hub.exe"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "UninstallString" '"$INSTDIR\Uninstall.exe"'
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "ModifyPath" '"$EXEPATH"'
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub" "RepairPath" '"$EXEPATH"'
  ${EndIf}
  core_end:
SectionEnd
SectionGroup "Offline games"
!include "${PACKSECTIONS}"
SectionGroupEnd
Section /o "Desktop shortcut"
  ${If} $OnePack == ""
  ${AndIf} $PortableTest == ""
    CreateShortcut "$DESKTOP\Catnmice Game Hub.lnk" "$INSTDIR\Catnmice Game Hub.exe"
  ${EndIf}
SectionEnd
Section "Uninstall"
  IfFileExists "$INSTDIR\catnmice-install.txt" 0 done
  ${If} $INSTDIR == "$LOCALAPPDATA\Programs\Catnmice Game Hub"
    Delete "$DESKTOP\Catnmice Game Hub.lnk"
    Delete "$SMPROGRAMS\Catnmice Game Hub\Catnmice Game Hub.lnk"
    RMDir "$SMPROGRAMS\Catnmice Game Hub"
    DeleteRegKey HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\CatnmiceGameHub"
    MessageBox MB_YESNO|MB_DEFBUTTON1 "Keep your local profiles and saved games?" /SD IDYES IDYES keep
      RMDir /r "$APPDATA\Catnmice Game Hub"
    keep:
    RMDir /r "$INSTDIR"
  ${EndIf}
  done:
SectionEnd
