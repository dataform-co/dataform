protoc -I=protos --python_out=pycore/protos/ protos/core.proto
python3.10 pycore "${@:1}"
