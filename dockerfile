FROM r-base
RUN apt update
RUN apt -y install libcurl4-openssl-dev libxml2-dev libjq-dev libv8-dev
RUN Rscript -e 'install.packages("remotes")'
RUN Rscript -e 'install.packages("readxl")'
RUN Rscript -e 'install.packages("readr")'
RUN Rscript -e 'install.packages("EML")'
RUN Rscript -e 'remotes::install_github("CVPIA-OSC/EMLaide")'
RUN Rscript -e 'install.packages("purrr")'
RUN apt -y install curl sudo
RUN apt -y install git



