import kaggle

kaggle.api.authenticate

kaggle.api.dataset_download_files('harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows', path='./resources', unzip=True)
kaggle.api.dataset_download_files('ashpalsingh1525/imdb-movies-dataset', path='./resources', unzip=True)
kaggle.api.dataset_download_files('thedevastator/netflix-imdb-scores', path='./resources', unzip=True)