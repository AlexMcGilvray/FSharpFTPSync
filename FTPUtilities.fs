namespace FSharpFTPLib


/// NOTES

// May check for the existance of all files on the source side on the ftp side just in case there's a file that was deleted.... 

module QuickLogger =
    open System.IO
    open System

    let internal filePath = DateTime.Now.ToString("yyyy-dd-mm-hh-MM-ss") + "_log.txt";
    let internal ignoreDebugLog = true
    //let internal fileStream = File.Create(filePath);
    //let internal logWriter = new StreamWriter(fileStream);

    let internal log (msg:string) =
        use logWriter = new StreamWriter(filePath,true);
        logWriter.WriteLine(msg.ToCharArray())
        printfn "%s" msg

    let logError msg =
        log ("Error: " + msg)

    let logWarning msg =
        log ("Warning: " + msg)

    let logInfo msg =
        log msg

    let logDebug msg =
        if not (ignoreDebugLog) then log msg
    

// notes & resources : 
//   FtpWebResponse class : https://docs.microsoft.com/en-us/dotnet/api/system.net.ftpwebresponse?view=netframework-4.8
//   How to find out if a uri on the server is a file or directory : http://www.copyandwaste.com/posts/view/parsing-webrequestmethodsftplistdirectorydetails-and-listdirectory/
module FTPUtilities = 
    open System.Security.Cryptography
    open System.IO
    open QuickLogger

    let manifestFileName = "ftp_sync_manifest.csv"

    [<AutoOpen>]
    module private FTPRequestCreationHelperFunctions = 
        open System.Net
        open System.Text.RegularExpressions
        
        let createFTPRequest requestMethod ftpUri login (pass:string) =
            let request = WebRequest.Create(ftpUri:System.Uri)
            request.Method <- requestMethod
            request.Credentials <- new NetworkCredential(login,pass)
            request

        let createFTPRequestListDirectoryDetails ftpUri login pass = 
            createFTPRequest WebRequestMethods.Ftp.ListDirectoryDetails ftpUri login pass
        
        type UploadStatus =
            | Success
            | Error of string

        // TODO : Need to attempt to make every sub-folder leading up to the final path
        // TODO : Test this and see if it actually works
        let makePath (ftpUri:System.Uri) login (pass:string) = 
            let ftpSplitRegexString = """(?<root>ftp:\/\/.*?\/)(?<rest>.*)"""
            let ftpSplitRegex = new Regex(ftpSplitRegexString)
            let ftpSplitMatch = ftpSplitRegex.Match(ftpUri.ToString())
            if ftpSplitMatch.Success then
                let pathsString = ftpSplitMatch.Groups.["rest"].Value
                let pathsArray = pathsString.Split('/')
                let ftpPathRoot = ftpSplitMatch.Groups.["root"].Value 
                let mutable ftpPathRest = ""
                for i in 0 .. pathsArray.Length - 1 do
                    try
                        ftpPathRest <- ftpPathRest + pathsArray.[i] + "/"
                        let ftpPath = ftpPathRoot + ftpPathRest
                        logDebug ("ftp path to create : " + ftpPath)
                        let request = createFTPRequest WebRequestMethods.Ftp.MakeDirectory (new System.Uri(ftpPath)) login pass
                        use responseStream = request.GetResponse().GetResponseStream()
                        use responseStreamReader = new StreamReader(responseStream)
                        let response = responseStreamReader.ReadToEnd()
                        logInfo response
                    with
                    | ex -> if not (ex.Message.Contains("550")) then logError ex.Message
            else 
                logWarning "match unsuccessful"

            logDebug ("\n\nattempting to create path " + ftpUri.ToString())
            createFTPRequest WebRequestMethods.Ftp.MakeDirectory ftpUri login pass
            
        let uploadFile sourcePath (ftpUri:string) login (pass:string) =
            try
                logInfo (sprintf "\nattempting to upload file %s to %s" sourcePath ftpUri)
                use client = new WebClient()
                client.Credentials <- new NetworkCredential(login,pass)
                client.UploadFile(ftpUri,WebRequestMethods.Ftp.UploadFile,sourcePath) |> ignore
                logInfo "\tSuccess!"
                Success
            with
            | ex -> 
                logError ex.Message
                Error ex.Message
                
    [<AutoOpen>]
    module private FileOperations = 
        open System.IO

        type OSFileInfo =
            { FileName:string }
        
        type OSFileInfoWithHash =
            { FileName:string
              Hash:byte[] }

        type OSDirectoryInfo = 
            { FileName:string 
              Children:seq<OSFileNode> }
        
        and OSFileNode =
            | OSFile of OSFileInfo
            | OSFileWithHash of OSFileInfoWithHash
            | OSDirectory of OSDirectoryInfo

        let getDirectoryStructure rootFolderPath = 
            let rec getDirectoryContents path = 
                let files = Directory.GetFiles(rootFolderPath) |> Seq.ofArray |> Seq.map (fun x ->  OSFileNode.OSFile{ FileName = x })
                let directories = Directory.GetDirectories(rootFolderPath) |> Seq.ofArray |> Seq.map (fun x -> OSFileNode.OSDirectory{ FileName = x; Children = getDirectoryContents x })
                Seq.append files directories
            getDirectoryContents rootFolderPath

        let getDirectoryStructureWithFileHashes rootFolderPath = 
            let sha256 = SHA256.Create()
            let rec getDirectoryContentsWithHash path = 
                let files = Directory.GetFiles(path) |> Seq.ofArray |> Seq.map (fun x -> OSFileNode.OSFileWithHash{ FileName = x; Hash = using (File.OpenRead x) sha256.ComputeHash})
                let directories = Directory.GetDirectories(path) |> Seq.ofArray |> Seq.map (fun x -> OSFileNode.OSDirectory{ FileName = x; Children = getDirectoryContentsWithHash x })
                Seq.append files directories
            getDirectoryContentsWithHash rootFolderPath
 
        let generateInMemoryManifestTextOutput rootFolderPath =
            let directoryStructure = getDirectoryStructureWithFileHashes rootFolderPath
            let rec traverse (fileNodes:seq<OSFileNode>) = 
                fileNodes 
                |> Seq.map (fun x -> 
                    match x with
                    | OSFile f -> 
                        let filePath = (f.FileName).Replace(rootFolderPath,"") + "\n"
                        filePath
                    | OSFileWithHash f -> 
                        let filePathWithHash = (f.FileName).Replace(rootFolderPath,"") + " " + (f.Hash |> Seq.ofArray |> Seq.map (fun x -> x.ToString()) |> Seq.reduce (+)) + "\n"
                        filePathWithHash
                    | OSDirectory d -> 
                        traverse d.Children )
                |> Seq.filter (fun x -> not (System.String.IsNullOrEmpty x) ) |> Seq.fold (+) ""
            traverse directoryStructure

        // note the filename is the full path. We need to strip out stuff to make all the paths relative to the rootFolderPath param
        let generateFileManifest rootFolderPath =
            let output = generateInMemoryManifestTextOutput rootFolderPath
            File.WriteAllText(rootFolderPath + "\\" + manifestFileName, output);
            ()

    [<AutoOpen>]
    module private FtpOperations = 
        open System
        open System.Net
        
        type FtpOperationResult =
            | Success of response:List<string>
            | Error of message:string * ex:Exception

        let getDirectoryList ftpUri login password = 
            let readLines (streamReader:StreamReader) = seq {
                while not streamReader.EndOfStream do
                    yield streamReader.ReadLine()
            }
            try
                let request = createFTPRequestListDirectoryDetails ftpUri login password
                use responseStream = request.GetResponse().GetResponseStream()
                use responseStreamReader = new StreamReader(responseStream)
                readLines responseStreamReader |> List.ofSeq |> Success
            with
            |  ex -> Error (ex.Message, ex)

        let doesFileUriExist ftpUri login password =
            let readLines (streamReader:StreamReader) = seq {
                while not streamReader.EndOfStream do
                    yield streamReader.ReadLine()
            }
            try
                let request = createFTPRequest WebRequestMethods.Ftp.GetFileSize ftpUri login password 
                use responseStream = request.GetResponse().GetResponseStream()
                use responseStreamReader = new StreamReader(responseStream)
                readLines responseStreamReader |> List.ofSeq |> Success
            with
            |  ex -> Error (ex.Message, ex)

        let getTextFileContents ftpUri login pass = 
            try
                let request = createFTPRequest WebRequestMethods.Ftp.DownloadFile ftpUri login pass
                use responseStream = request.GetResponse().GetResponseStream()
                use responseStreamReader = new StreamReader(responseStream)
                responseStreamReader.ReadToEnd()
            with
            |  ex -> "error"

    /// <summary> Prints the contents of a directory non-recursively to the standard output </summary>
    let printDirectoryList ftpUri login pass = 
        match getDirectoryList ftpUri login pass with
        | Error (message, ex) -> printf "%s\n" message
        | Success response -> response |> List.iter (printf "%s\n")

    [<AutoOpen>]
    module private FtpFileSystemOperations = 
        open System
        open System.Text.RegularExpressions

        type FtpFileInfo =
            { FileName:string }

        type FtpDirectoryInfo = 
            { FileName:string 
              Children:seq<FtpFileNode> }
        
        and FtpFileNode =
            | File of FtpFileInfo * NodeUri:Uri
            | Directory of FtpDirectoryInfo * NodeUri:Uri

        type ListOutputRowMatch = 
            { IsDirectory : bool
              FileName: string }

        let parseFtpListCommandDetailsRow rowString =
            // ^(?<dir>[-d])(?<permission>(?:[-r][-w][-xs]){3})\s+\d+\s+\w+(?:\s+\w+)?\s+(?<size>\d+)\s+(?<timestamp>\w+\s+\d+(?:\s+\d+(?::\d+)?))\s+(?!(?:\.|\.\.)\s*$)(?<name>.+?)\s*$
            // https://stackoverflow.com/questions/42956894/regex-to-parse-ftp-server-list-c-sharp
            let ftpListLineRegexParseString = 
                """^(?<dir>[-d])(?<permission>(?:[-r][-w][-xs]){3})\s+\d+\s+\w+(?:\s+\w+)?\s+(?<size>\d+)\s+(?<timestamp>\w+\s+\d+(?:\s+\d+(?::\d+)?))\s+(?!(?:\.|\.\.)\s*$)(?<name>.+?)\s*$"""
            let ftpListRegex = new Regex(ftpListLineRegexParseString,RegexOptions.Compiled ||| RegexOptions.Multiline)
            let ftpFilematch = ftpListRegex.Match(rowString) 
            try
                let permissionNameMatchValues = if ftpFilematch.Success 
                                                then (ftpFilematch.Groups.["dir"].Value.[0], ftpFilematch.Groups.["name"].Value)  |> Some 
                                                else None
                let permissionMatchResult = 
                    match permissionNameMatchValues with
                    | Some x -> 
                        match x with 
                        | ('d',_) -> { IsDirectory = true; FileName = x  |> snd } |> Some
                        | ('-',_) -> { IsDirectory = false; FileName = x |> snd } |> Some
                        | _ -> None
                    | None -> None
                permissionMatchResult
            with
            | ex -> None

        let getFileNodes login pass ftpUri = 
            match getDirectoryList ftpUri login pass with
            | Error (message, ex) -> None
            | Success response -> 
                response |> 
                List.choose (fun x -> parseFtpListCommandDetailsRow x ) |> 
                Seq.map (fun x -> 
                            if x.IsDirectory 
                            then FtpFileNode.Directory ({ FileName = x.FileName; Children = List.empty}, NodeUri = new Uri(ftpUri.ToString() + "/" + x.FileName))
                            else FtpFileNode.File ({ FtpFileInfo.FileName = x.FileName}, NodeUri =  new Uri(ftpUri.ToString() + "/" + x.FileName))) |> Some

        let getFtpFileSystemStructure ftpUri login pass =
            let getFileNodesWithPermission nodeUri = 
                match getFileNodes login pass nodeUri with
                | Some x -> x
                | None -> Seq.empty
            let rootFileNodes = getFileNodesWithPermission ftpUri
            let rootNode = FtpFileNode.Directory ({ FileName = "."; Children = rootFileNodes}, NodeUri = ftpUri)
            let rec traverse ftpRootFileNode =
                match ftpRootFileNode with
                | Directory (node,uri) ->
                    let children = getFileNodesWithPermission uri |> Seq.map traverse
                    FtpFileNode.Directory ({ FileName = node.FileName; Children = children}, NodeUri = uri)
                | File (node,uri) -> ftpRootFileNode
            traverse rootNode
        
        let printFtpFileSystemStructure login pass ftpUri =
            let fileSystemStructure = getFtpFileSystemStructure ftpUri login pass 
            let rec traverse ftpRootFileNode indentationLevel = 
                let indentation = seq {for i in 0 .. indentationLevel do yield " "} |> Seq.fold (+) ""
                match ftpRootFileNode with
                | Directory (node,uri) ->
                    node.Children |> Seq.iter (fun x -> (traverse x (indentationLevel + 1))) |> ignore
                    printf "\n%s %s" indentation node.FileName |> ignore
                | File (node,uri) ->
                    printf "\n%s %s" indentation node.FileName |> ignore
            traverse fileSystemStructure 0

    let printFTPStructure login pass ftpUri = printFtpFileSystemStructure login pass ftpUri |> ignore

    let printFileStructure rootFolderPath =
        let fileStructure = getDirectoryStructure rootFolderPath
        let rec traverse (fileNodes:seq<OSFileNode>) =
            fileNodes |> Seq.iter (fun x -> 
                printf "\n"
                match x with
                | OSDirectory node ->
                    traverse node.Children |> ignore
                    printf "Directory: %s" node.FileName
                | OSFile node ->
                    printf "File: %s" node.FileName
                | OSFileWithHash node ->
                    printf "File: %s Hash: %s" node.FileName (node.Hash |> Seq.ofArray |> Seq.map (fun x -> x.ToString()) |> Seq.reduce (+))
                )
        traverse fileStructure |> ignore

    let printFileWithHashStructure rootFolderPath = 
        let fileStructure = getDirectoryStructureWithFileHashes rootFolderPath
        let rec traverse (fileNodes:seq<OSFileNode>) =
            fileNodes |> Seq.iter (fun x -> 
                printf "\n"
                match x with
                | OSDirectory node ->
                    traverse node.Children |> ignore
                    printf "Directory: %s" node.FileName
                | OSFile node ->
                    printf "File: %s" node.FileName
                | OSFileWithHash node ->
                    printf "File: %s Hash: %s" node.FileName (node.Hash |> Seq.ofArray |> Seq.map (fun x -> x.ToString()) |> Seq.reduce (+))
                )
        traverse fileStructure |> ignore

    let generateFileManifest rootFolder = generateFileManifest rootFolder

    let parseManifest (manifestBody:string) = 
        let manifestTextPairs = manifestBody.Split('\n')
        manifestTextPairs |> Array.map(fun x -> x.Split(' ')) |> Array.filter (fun element -> Array.length element = 2) |> Array.map (fun xs -> (xs.[0], xs.[1]) )
    
    let findValueInManifest (manifest:(string * string) []) (lookupValue:string * string) = 
        Array.tryFind (fun x -> fst x = fst lookupValue && snd x = snd lookupValue) manifest

    // TODO Check to make sure the path exists before uploading, FTP looks like it won't create the full path for you on upload if it doesn't exist.
    let syncFTP sourceFolder targetFTPUri login pass = 
        let manifestFTPUri = new System.Uri(targetFTPUri + "/" + manifestFileName)
        // determine if there is a manifest in the root folder
        let doesManifestExistInSource = System.IO.File.Exists(System.IO.Path.Combine(sourceFolder,manifestFileName))
        // determine if there is a manifest in the target ftp uri
        let doesManifestExistInFTP =
            match doesFileUriExist manifestFTPUri login pass  with 
            | FtpOperationResult.Success s -> true
            | FtpOperationResult.Error (f,e) -> false
        
        // make new manifest for source
        let sourceManifestText = generateInMemoryManifestTextOutput sourceFolder
        printfn "%s" sourceManifestText
        let sourceManifest = parseManifest sourceManifestText

        // download and parse the target manifest
        let targetManifestText = getTextFileContents manifestFTPUri login pass
        let targetManifest = parseManifest targetManifestText
        // for each entry in the source manifest, see if there's a matching entry in the target manifest and filter if there is
        let filesToUpload = sourceManifest |> Array.filter (fun x -> 
            match findValueInManifest targetManifest x with 
            | Some (x,z) -> false
            | None -> true)
        let printLines (streamReader:StreamReader) =
            while not streamReader.EndOfStream do
                printfn "%s\n" (streamReader.ReadLine())
        // upload files that are determined to be missing or different
        filesToUpload |> Array.iter (fun x -> 
            let subPath = (fst x).Substring(1,(fst x).Length - 1)
            let sourceFile = Path.Combine (sourceFolder,subPath)
            let targetUri = (targetFTPUri + (fst x).Replace("\\","/").Replace("//","/"))
            let nonFileTargetUriFolder = targetUri.Substring(0, targetUri.LastIndexOf('/'))
            try
                // TODO  : String the file from the end of the targetUri path so we can attempt to create the folder structure on the ftp side before uploading the file.
                let makePathRequest = makePath (new System.Uri(nonFileTargetUriFolder)) login pass
                use responseStream = makePathRequest.GetResponse().GetResponseStream()
                use responseStreamReader = new StreamReader(responseStream)
                printLines responseStreamReader
            with
                |  ex -> printfn "\tError : %s " |> ignore // TODO convert to use logger
            uploadFile sourceFile targetUri login pass |> ignore)
        // save the source manifest locally
        let sourceManifestFileLocation = sourceFolder + @"/" + manifestFileName
        File.WriteAllText(sourceManifestFileLocation,sourceManifestText)
        // upload the manifest
        let targetManifestFTPLocation = (targetFTPUri + "/" + manifestFileName)
        printfn "\nuploading manifest from from %s to %s" sourceManifestFileLocation targetManifestFTPLocation
        uploadFile sourceManifestFileLocation targetManifestFTPLocation login pass |> ignore
        // TODO keep track of files that failed to upload and remove their entries from the manifest

        // TODO need to ensure files exist on target ftp

        // upload manifest
        // uploadFile sourceFile targetUri login pass |> ignore

        0 |> ignore

            // upload everything in the source folder to the target uri
        // if either is false then check to see if targetFTPUri is an empty directory
            // if not then fail and return error (handle it nicer later)
        // if directory is empty
            // generate new source manifest in source root folder
            // upload everything from source directory to target ftp uri

        0