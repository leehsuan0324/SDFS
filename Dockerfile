FROM golang:latest
WORKDIR /code
COPY . /code/
# CMD cd /code
CMD ["sleep", "infinity"]
#CMD [ "go", "run" ,"./src/server/*.go", "${MACHINE_NUM}" ]