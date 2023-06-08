#!/usr/bin/env python
# coding: utf-8
"""
Python dictionary that specifies file categories and the file extensions that belong
in each category

This script can be run directly to replace the existing file category filters in a
Kibana saved object
"""
# fmt: off
__title__         = "gen_file_category_ndjson"
__version__       = "1.0.0"
__date__          = "08 June 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
# fmt: off
FILE_CATEGORIES = {
    "Application and library files": [
        "com",      # DOS command file
        "dll",      # Windows Dynamic Link Libraries
        "exe",      # Windows Executable
        "ko",       # Linux kernel module
        "lib",      # Library
        "msi",      # Microsoft Installer
        "so",       # Linux shared object
    ],
    "Audio files": [
        "3ga",      #
        "aa",       # *Audible Audio book
        "aac",      #
        "aif",      #
        "aiff",     #
        "ape",      #
        "au",       #
        "cda",      #
        "cdda",     #
        "flac",     # *FLawless Audio Codec
        "m4a",      # MPEG-4 audio
        "mid",      # MIDI
        "midi",     # MIDI
        "mka",      # Matroska Audio
        "mod",      # Amiga Music Module file
        "mp3",      #
        "mpa",      #
        "oga",      # Ogg Vorbis audio
        "ra",       # Real Audio
        "ram",      # Real Audio
        "snd",      # Generic sound file
        "wav",      # Generic wave form audio
        "wave",     # Generic wave form audio
        "wma",      # Windows Media Audio
    ],
    "Compressed files": [
        "7z",       # 7Zip
        "ace",      #
        "afa",      #
        "alz",      #
        "apk",      # Android PacKage archive
        "ar",       #
        "arc",      #
        "ark",      #
        "arj",      #
        "b1",       # B1
        "b6z",      # B6z
        "bz2",      # Bzip2
        "bzip2",    # Bzip2
        "cab",      # Cabinet file
        "cb7",      # Comic Book 7Zip Archive
        "cba",      # Comic Book ACE Archive
        "cbt",      # Comic Book TAR Archive
        "cbz",      # Comic Book Zip Archive
        "cdx",      #
        "cfs",      # *Compact File Set
        "cpio",     #
        "cpt",      # Compact Pro
        "dar",      # *Disk ARchiver
        "dd",       # *Disk Doubler
        "deb",      # Debian Software Archive
        "ear",      # *Enterprise java ARchive
        "gz",       # GZip
        "gzip",     # GZip
        "jar",      # *Java ARchive
        "lbr",      #
        "lha",      #
        "llzh",     #
        "lz",       #
        "lzh",      #
        "lzma",     #
        "pea",      # PeaZip
        "rar",      #
        "rpm",      # Redhat Package Manager file
        "rz",       # RZip
        "s7z",      #
        "sfx",      # Self extracing archive
        "shar",     # *SHell ARchive
        "sit",      # StuffIt
        "sitx",     # StuffIt X
        "sz",       #
        "tar",      # *Tape ARchive
        "taz",      #
        "tbz",      # TAR Bzip
        "tbz2",     # TAR Bzip2
        "tgz",      # TAR Gzip
        "tpz",      #
        "war",      # Java Web Archive
        "xz",       #
        "z",        # Zip
        "zip",      # Zip archive
        "zoo",      #
        "zz",       #
    ],
    "Disc images": [
        "fdd",      # Parallels floppy disk image
        "hdd",      # Parallels disk image
        "hds",      # Parallels disk image
        "iso",      # ISO image of CD/DVD/BluRay
        "dvd",      #
        "dmg",      # Apple disk image
        "dmgpart",  # Apple disk image part
        "partimg",  # PartImage
        "qcow",     # QEMU disk image
        "qcow2",    # QEMU disk image
        "toast",    # Roxio Toast
        "vcd",      # *Video CD
        "vfd",      # *Virtual Floppy Disk
        "vdi",      # *Virtual Disk Image
        "vhd",      # Hyper-V disk image
        "vhdx",     # Hyper-V disk image
        "vmdk",     # VMware disk image
    ],
    "Data and database files": [
        "accdb",    # Microsoft Access
        "accdt",    # Microsoft Access
        "ade",      #
        "adp",      #
        "db",       # Generic database file
        "db2",      #
        "db3",      #
        "dbs",      #
        "json",     # *JSON file
        "mdb",      #
        "ndjson",   # *Newline Delimited JSON
        "sqlite",   # SQLite
    ],
    "Document files": [
        "123",      # Lotus 1-2-3
        "12m",      # Lotus 1-2-3
        "1st",      # Readme file
        "ans",      # ANSI text file
        "asc",      # ASCII text file
        "csv",      # Comma Separated Values
        "doc",      # Microsoft Word Document
        "docm",     # Microsoft Word Open XML Macro Enabled Document
        "docx",     # Microsoft Word Open XML Document
        "dotx",     # Microsoft Template
        "epub",     # *Electronic PUBlication
        "fods",     # OpenDocument Flat XML Spreadsheet
        "latex",    # LATEX document
        "odm",      # OpenDocument Master Document
        "ods",      # OpenDocument Spreadsheet
        "odt",      # OpenDocument Text Document
        "ots",      # OpenDocument Spreadsheet Template
        "pdf",      # Portable Document Exchange file
        "pot",      # Microsoft PowerPoint template
        "potm",     # Microsoft Open XML Macro Enabled PowerPoint template
        "potx",     # Microsoft Open XML PowerPoint template
        "pps",      # Microsoft PowerPoint slideshow
        "ppsx",     # Microsoft Open XML PowerPoint slideshow
        "ppt",      # Microsoft PowerPoint
        "pptm",     # Microsoft Open XML Macro Enabled PowerPoint
        "pptx",     # Microsoft Open XML PowerPoint file
        "rtf",      # *Rich Text Format
        "tex",      # LaTeX document
        "text",     # Text file
        "txt",      # Text file
        "wks",      # Lotus 1-2-3
        "wku",      # Lotus 1-2-3
        "wpd",      # WordPerfect document
        "wps",      # Microsoft Works
        "xla",      # Microsoft Excel
        "xlam",     # Microsoft Excel
        "xlm",      # Microsoft Excel Macro
        "xls",      # Microsoft Excel binary format
        "xlsb",     # Microsoft Excel
        "xlsm",     # Microsoft Excel Open XML Macro Enabled Spreadsheet
        "xlsx",     # Microsoft Excel Open XML Spreadsheet
        "xlt",      # Microsoft Excel Template
        "xltm",     # Microsoft Excel Open XML Macro Enabled Template
        "xltx",     # Microsoft Excel Open XML Template
    ],
    "E-mail, calendar, and contacts files": [
        "edb",      # Microsoft Exchange DB
        "email",    #
        "eml",      # Standard email file
        "emlx",     # Apple extended email file
        "ics",      # iCalendar file format
        "mbox",     #
        "msg",      # Microsoft Outlook
        "oft",      # Microsoft Outlook Email Template
        "olm",      # Microsoft Outlook for Mac
        "ost",      # Microsoft Outlook
        "p7s",      # Digitally signed email
        "pst",      # Microsoft Outlook
        "rpmsg",    # Microsoft Outlook Restricted Permission Message
        "tnef",     #
        "vcf",      # *Virtual Contact File
    ],
    "Image files": [
        "bmp",      # *BitMaP
        "bpg",      # *Better Portable Graphics
        "cr2",      # Canon RAW format
        "cur",      # *CURsor
        "dcm",      # *Digital Imaging and Communications in medicine image
        "dicom",    # *Digital Imaging and Communications in medicine image
        "dng",      # Generic RAW image format
        "eps",      # *Encapsulated PostScript
        "fit",      # *Flexible Image Transport system
        "fits",     # *Flexible Image Transport System
        "flif",     # *Free Lossless Image Format
        "g3",       # *G3 fax image
        "gif",      # *Graphics Interchange Format
        "ico",      # *ICOn
        "icon",     # Generic icon
        "img",      # IMaGe (Various)
        "jpeg",     # *Joint Pictures Experts Group
        "jpg",      # *Joint Pictures experts Group
        "nef",      # Nikon RAW format
        "pam",      # *Portable Arbitrary Map
        "pbm",      # *Portable Bit Map
        "pcc",      # ZSoft PCX Image
        "pcx",      # ZSoft PCX Image
        "pgm",      # *Portable Gray Map
        "png",      # *Portable Network Graphics
        "pnm",      # *Portable aNy Map
        "pns",      # PNG Stereo
        "ppm",      # *Portable Pixel Map
        "ps",       # *PostScript
        "ras",      # *RASter image file
        "raw",      # Generic RAW image format
        "rs",       # Raster image file
        "svg",      # *Scalable Vector Graphics
        "tga",      # #Truevision Graphics Adapter
        "tif",      # *Tagged Image File format
        "tiff",     # *Tagged Image File Format
        "webp",     #
        "xbm",      # *X BitMap
        "xcf",      # Native GIMP image format
        "xpm",      # *X PixelMap
        "xwd",      # X-Windows Dump
    ],
    "Script files": [
        "bat",      # Batch script
        "bash",     # Bash script
        "cdxml",    # PowerShell script
        "csh",      # CShell script
        "lasso",    # Lasso
        "lua",      # LUA
        "phar",     # PHP
        "php",      # PHP
        "php-s",    # PHP
        "php3",     # PHP
        "php4",     # PHP
        "php5",     # PHP
        "php7",     # PHP
        "phps",     # PHP
        "pht",      # PHP
        "phtml",    # PHP
        "pl",       # Pearl
        "plx",      # Pearl
        "pm",       # Pearl
        "pod",      # Pearl
        "ps1",      # PowerShell script
        "ps1xml",   # PowerShell script
        "psc1",     # PowerShell script
        "psd1",     # PowerShell script
        "psm1",     # PowerShell script
        "pssc",     # PowerShell script
        "py",       # Python
        "pyc",      # Python
        "pyd",      # Python
        "pyi",      # Python
        "pyw",      # Python
        "pyz",      # Python
        "rb",       # Ruby
        "sh",       # SHell script
        "t",        # Pearl
        "tcl",      # TCL
        "tbc",      # TCL
        "vc",       # Visual Basic
        "vbs",      # Visual Basic
        "xs",       # Pearl
        "zsh",      # ZShell script
    ],
    "Source code files": [
        "asm",      # General assembly code
        "c",        # C and C++
        "c++",      # C and C++
        "cc",       # C and C++
        "class",    # Java
        "cpp",      # C and C++
        "cs",       # C#
        "css",      # HTML
        "cxx",      # C and C++
        "f",        # Fortran
        "for",      # Fortran
        "f90",      # Fortran
        "go",       # Go
        "html",     # HTML
        "h",        # C and C++
        "hpp",      # C and C++
        "hxx",      # C and C++
        "java",     # Java
        "js",       # Javascript
        "jsp",      # *Java Server Page
        "rlib",     # Rust
        "rs",       # Rust
        "swift",    # Swift
        "v",        # Verilog
        "vh",       # Verilog
        "vhdl",     # VHDL
    ],
    "Video files": [
        "264",      # h.264 video file
        "3g2",      # Mobile phone video format
        "3gp",      # Mobile phone video format
        "3gp2",     # Mobile phone video format
        "3gpp",     # Mobile phone video format
        "3gpp2",    # Mobile phone video format
        "60d",      # Wavereader CCTV video file
        "ajp",      # CCTV video file
        "am4",      # Security First CCTV video file
        "amv",      # Modified version of AVI
        "arv",      # Everfocus CCTV video file
        "asd",      # Microsoft advanced streaming format
        "asf",      # *Advanced Systems Format
        "avb",      # Avid bin file format
        "avd",      # Avid video file
        "avi",      # *Audio Video Interleved
        "drc",      # Dirac
        "f4b",      # Flash video
        "f4p",      # Flash video
        "f4v",      # Flash video
        "flv",      # Flash video
        "h264",     # h264 encoded video
        "m2ts",     # *Mpeg-2 Transport Stream
        "m2v",      # MPEG-2
        "m4p",      # MPEG-4 video with DRM
        "m4v",      # MPEG-4 video
        "mbf",      # CCTV video file
        "mkv",      # *MatosKa Video
        "mov",      # QuickTime file format
        "mp2",      # MPEG-1
        "mp4",      # *Mpeg-4
        "mpe",      # MPEG-1
        "mpeg",     # MPEG-1/MPEG-2
        "mpg",      # MPEG-1/MPEG-2
        "mpv",      # MPEG-1
        "mts",      # *Mpeg Transport Stream
        "nsv",      # *Nullsoft Streaming Video
        "ogg",      # *OGG vorbis
        "ogv",      # *OGg vorbis Video
        "ogx",      # *OGg vorbis multipleXed media file
        "pns",      # Pelco PNS CCTV video file
        "qt",       # *QuickTime file format
        "rm",       # *RealMedia
        "rmvb",     # *RealMedia Variable Bitrate
        "sdv",      # *Samsung Digital Video
        "svi",      # Samsung video format
        "tivo",     # TIVO video file
        "tod",      # JVC camcorder video file
        "ts",       # *mpeg-2 Transport Stream
        "vmb",      # CCTV video file
        "vob",      # MPEG based video format for DVDs
        "webm",     # *WebM
        "wmv",      # *Windows Media Video
        "xvid",     # XviD encoded video file
    ],
}
# fmt: on


def main():
    import sys
    from helpers.vis_gen_helpers import generate_file_category_ndjson

    num_args = len(sys.argv)
    if num_args < 2 or num_args > 3:
        sys.stderr.write("Usage: gen_file_category_ndjson <input_template> [<output_file_defaults_to_stdout>]\n")
        sys.exit(1)
    infile = open(sys.argv[1], "r")
    outfile = sys.stdout if num_args == 2 else open(sys.argv[2], "w")
    template_array = infile.readlines()
    infile.close()
    ndjson_array = generate_file_category_ndjson(template_array, FILE_CATEGORIES)
    for line in ndjson_array:
        outfile.write(line + "\n")
    outfile.close()


if __name__ == "__main__" or __file__ == None:
    main()
