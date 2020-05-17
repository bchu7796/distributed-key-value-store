LDFLAGS = -L/usr/local/lib, `pkg-config --libs protobuf grpc++`\
          -lgrpc++_reflection\
          -ldl -lpthread\


CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++11 

GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

ODIR = obj
SERVER = $(wildcard ./build/server)
CLIENT = $(wildcard ./build/client)
SRC1 = $(wildcard ./abd_client/*.cc)
SRC2 = $(wildcard ./abd_server/*.cc)
SRC3 = $(wildcard ./tools/*.cc)
SRC4 = $(wildcard ./kv_store/*.cc)

ABD_CLIENT_OBJ = $(patsubst %.cc,%.o,$(SRC1))
ABD_SERVER_OBJ = $(patsubst %.cc,%.o,$(SRC2))
TOOLS_OBJ = $(patsubst %.cc,%.o,$(SRC3))
KV_STORE_OBJ = $(patsubst %.cc,%.o,$(SRC4))
GRPC1_OBJ = abdserver.pb.o
GRPC2_OBJ = abdserver.grpc.pb.o

all: ./build/abd_server ./build/abd_client

./build/abd_server: $(GRPC1_OBJ) $(GRPC2_OBJ) $(ABD_SERVER_OBJ) $(TOOLS_OBJ) $(KV_STORE_OBJ)
	$(CXX) $^ $(LDFLAGS) -o $@

./build/abd_client: $(GRPC1_OBJ) $(GRPC2_OBJ) $(ABD_CLIENT_OBJ) $(TOOLS_OBJ)
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	protoc --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h $(GRPC1_OBJ) $(GRPC2_OBJ) $(ABD_SERVER_OBJ)\
	                         $(TOOLS_OBJ) $(KV_STORE_OBJ) $(ABD_CLIENT_OBJ)\
							 ./build/kv_store ./build/log\
							 ./build/abd_server ./build/abd_client\