FROM continuumio/anaconda3
LABEL maintainer="sagang"
COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt \
    && mkdir /notebooks \
    && conda install -c sas-institute swat
EXPOSE 8888 5571 80 8777
CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root","--NotebookApp.token=''","--NotebookApp.password=''"]