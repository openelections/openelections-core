## url\_paths.csv

In addition to the fields found in other states, I added some extra columns.

The main reason for these fields is that there are some elections where there are precint-level results, but for many counties, there aren't any offices of interest.  I wanted to have a record that the files existed, but a mechanism to exlcude them from the datasource mappings. 

The additional columns are:

* filename: Raw filename.  This was added in case we need to regenerate the URLs somehow or to avoid URL parsing.
* has\_statwide\_results: Does this file contain statewide results that OpenElections is interested in?
* skip: Should this file be skipped when defining datasource mappings?
* needs\_preprocessing: File needs to be preprocessed before it can be loaded, usually because it's a PDF.
* raw\_extracted\_filename: File within an archive that will ultimately be extracted and saved to the cache.
* parent\_zipfile: Some results zips have two-levels of zip files.  We need to know the extracted filenames parent zip archive to be able to properly cache the file.


