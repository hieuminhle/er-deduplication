# DIA-ER
This Projects aims to learn the process of entity resolution and this includes the steps: data preparation, blocking data, match the data
## I get 'Permission Denied' error.
Have you made sure you set the file permission to 'Anyone with Link'?

## I set the permission 'Anyone with Link', but still can't download.
Google restricts access to a file when the download is concentrated. If you can still access to the file from your browser, downloading cookies file might help.
Follow this step:
1) download cookies.txt using browser extensions like (Get cookies.txt LOCALLY);
2) mv the cookies.txt to ~/.cache/gdown/cookies.txt;
3) run download again. If you're using gdown>=5.0.0, it should be able to use the cookies same as your browser.