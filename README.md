## webgme_cleanup
Removes unused data from a WebGME Mongodb collection

Usage:
Make a backup of your Mongodb database:  

    mongodump

Run webgme_cleanup with `--squash`, which removes all commit history:  

    node index.js --db CyPhy --collection WebGMEProjectName  --squash
