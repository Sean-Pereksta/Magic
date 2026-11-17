Set files = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
root = files.GetParentFolderName(WScript.ScriptFullName)
runtime = files.BuildPath(root, "runtime\node.exe")
launcher = files.BuildPath(root, "server\launcher.mjs")
shell.Run Chr(34) & runtime & Chr(34) & " " & Chr(34) & launcher & Chr(34), 0, False
