"""Versjon for Generic Decrypt-utviklingsgrenen.

VERSION er ordinær applikasjonsversjon for oppdatering/PREMIS.
DEVELOPMENT_SUFFIX styrer bare synlig utviklingsmerking.
"""

VERSION = "1.4.22"
DEVELOPMENT_SUFFIX = "ikamr-03"
DISPLAY_VERSION = f"{VERSION}-{DEVELOPMENT_SUFFIX}" if DEVELOPMENT_SUFFIX else VERSION
