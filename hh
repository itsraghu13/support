import gzip
import re
import glob

def search_gz_files(search_string, file_list):
  """
  Searches a list of gzipped files for the given search string.

  Args:
    search_string: The search string to look for.
    file_list: A list of gzipped file paths.

  Returns:
    A list of file paths that contain the search string.
  """

  results = []
  for file_path in file_list:
    with gzip.open(file_path, 'rb') as f:
      for line in f:
        if re.search(search_string, line):
          results.append(file_path)

  return results


if __name__ == '__main__':
  search_string = r'.*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z).*'
  file_list = glob.glob('*.gz')
  results = search_gz_files(search_string, file_list)

  for file_path in results:
    print(file_path)


























'".*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z).*"'

$folderPath = "$PSHOME\en-US\TimeLogs"
$searchPattern = '"(?<time>(?:YYYY-MM-DDT|YYYY-MM-DD )\d{2}:\d{2}:\d{2}\.\d{3}(Z|))"'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}












$folderPath = "$PSHOME\en-US\TimeLogs"
$searchPattern = '(?<time>(?:YYYY-MM-DDT|YYYY-MM-DD )\d{2}:\d{2}:\d{2}\.\d{3}(Z|))'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}










'(?<time>(?:YYYY-MM-DDT|YYYY-MM-DD )\d{2}:\d{2}:\d{2}\.\d{3}(Z|))'
 @('YYYY-MM-DDT00:00:00.000Z', 'YYYY-MM-DDT00:00:00.000', 'YYYY-MM-DD 00:00:00.000Z')

'(T\d{2}:\d{2}:\d{2}\.\d{3}(?!Z)|.*\d{2}:\d{2}:\d{2}\.\d{3}|T\d{2}:\d{2}:\d{2}\.\d{3})'
$folderPath = "$PSHOME\en-US\TimeLogs"
$searchPattern = '.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|(?<time>T\d{2}:\d{2}:\d{2}\.\d{3})|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern -and $_ -notmatch "Time Zone" }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}












'.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}\Z|\\T\d{2}:\d{2}:\d{2}\.\d{3}|\\.*\d{2}:\d{2}:\d{2}\.\d{3}\Z)\b.*'


    '.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|(?<time>T\d{2}:\d{2}:\d{2}\.\d{3})|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*'




$folderPath = "$PSHOME\en-US"
$searchPattern = '.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}




















'^.*\b(T\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z)\b.*$'



'^.*\bT\d{2}:\d{2}:\d{2}\.\d{3}Z\b|\bT\d{2}:\d{2}:\d{2}\.\d{3}\b|\b\d{2}:\d{2}:\d{2}\.\d{3}Z\b.*$'


$folderPath = "$PSHOME\en-US"
$searchPattern = 'T\d{2}:\d{2}:\d{2}\.\d{3}Z'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}



$folderPath = "$PSHOME\en-US"
$searchPattern = '^.*T\d{2}:\d{2}:\d{2}\.\d{3}Z.*$|^.*T\d{2}:\d{2}:\d{2}\.\d{3}$|^.*\d{2}:\d{2}:\d{2}\.\d{3}Z.*$'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line(s) found in $($filePath):"
        $matchingLines | ForEach-Object {
            Write-Host $_
        }
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}





Select-String -Path "$PSHOME\en-US\*.txt" -Pattern '^(T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}Z)$'''



$folderPath = "$PSHOME\en-US"
$searchPattern = '^(T\d{2}:\d{2}:\d{2}\.\d{3}|.*\d{2}:\d{2}:\d{2}\.\d{3}Z|T\d{2}:\d{2}:\d{2}\.\d{3}Z)$'

$matchingFiles = @()

Get-ChildItem -Path $folderPath -Filter *.txt | ForEach-Object {
    $filePath = $_.FullName
    Write-Host "Searching in $($filePath)"

    $fileContent = Get-Content $filePath
    $matchingLines = $fileContent | Where-Object { $_ -match $searchPattern }
    
    if ($matchingLines.Count -gt 0) {
        Write-Host "Matching line found in $($filePath):"
        $matchingLines | Select-Object -First 1
        $matchingFiles += $_
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing matching lines:"
    $matchingFiles | ForEach-Object {
        Write-Host $_.FullName
    }
} else {
    Write-Host "No files containing matching lines found."
}




# Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        $extractedPath = Join-Path $folderPath $gzFile.BaseName
        [System.IO.Compression.ZipFile]::ExtractToDirectory($gzFile.FullName, $extractedPath)

        $extractedFiles = Get-ChildItem -Path $extractedPath -File
        foreach ($extractedFile in $extractedFiles) {
            $fileContent = Get-Content $extractedFile.FullName
            if ($fileContent -match $searchPattern) {
                $matchingFiles += $gzFile
                break
            }
        }

        Remove-Item -Path $extractedPath -Recurse -Force
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}



















# Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        $stream = [System.IO.File]::OpenRead($gzFile.FullName)
        $gzStream = New-Object System.IO.Compression.GZipStream($stream, [System.IO.Compression.CompressionMode]::Decompress)
        $reader = New-Object System.IO.StreamReader($gzStream)
        $fileContent = $reader.ReadToEnd()
        $reader.Close()
        $gzStream.Close()
        $stream.Close()

        if ($fileContent -match $searchPattern) {
            $matchingFiles += $gzFile
        }
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}




Install-Module -Name System.IO.Compression.FileSystem








    # Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        $fileContent = Get-Content -Path $gzFile.FullName -Raw
        if ($fileContent -match $searchPattern) {
            $matchingFiles += $gzFile
        }
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}








# Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        # Use a chunk size of 1 MB
        $chunkSize = 1MB
        
        # Get the file size
        $fileSize = $gzFile.Length
        
        # Read the file in chunks
        for ($offset = 0; $offset -lt $fileSize; $offset += $chunkSize) {
            $chunk = Get-Content -Path $gzFile.FullName -ReadCount $chunkSize -Raw
            
            if ($chunk -match $searchPattern) {
                $matchingFiles += $gzFile
            }
        }
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}











# Define the folder path
$folderPath = "C:\path\to\your\folder"

# Define the search pattern
$searchPattern = "T\d{2}:\d{2}:\d{2}\.\d{3}$|.*\d{2}:\d{2}:\d{2}\.\d{3}Z$|T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

# Get a list of .gz files in the folder
$gzFiles = Get-ChildItem -Path $folderPath -Filter *.gz

# Initialize an array to store matching files
$matchingFiles = @()

# Loop through each .gz file and check for the search pattern
foreach ($gzFile in $gzFiles) {
    Write-Host "Searching in $($gzFile.Name)"
    
    try {
        # Use a chunk size of 1 MB
        $chunkSize = 1MB
        
        # Get the file size
        $fileSize = $gzFile.Length
        
        # Read the file in chunks
        for ($offset = 0; $offset -lt $fileSize; $offset += $chunkSize) {
            $chunk = Get-Content -Path $gzFile.FullName -ReadCount $chunkSize -Raw
            
            if ($chunk -match $searchPattern) {
                $matchingFiles += $gzFile
            }
        }
    } catch {
        Write-Host "Error processing $($gzFile.Name): $_"
    }
}

# Get the top 10000 matching files
$matchingFiles = $matchingFiles | Where-Object {$_ -le 10000}

if ($matchingFiles.Count -gt 0) {
    Write-Host "Files containing rows with the specified format:"
    foreach ($file in $matchingFiles) {
        Write-Host $file.FullName
    }
} else {
    Write-Host "No files containing rows with the specified format found."
}

