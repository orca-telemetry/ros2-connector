

## Nanoarrow config
Inside the nano arrow repo, run:

$ python ci/scripts/bundle.py \
  --source-output-dir=../ros-collector/vendor/nanoarrow \
  --include-output-dir=../ros-collector/vendor/nanoarrow \
  --header-namespace= \
  --with-ipc \
  --with-flatcc
