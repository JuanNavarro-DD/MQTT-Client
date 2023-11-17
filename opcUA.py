import asyncio
import logging
from asyncua import Client
logger = logging.getLogger('asyncua')
logging.disable(logging.WARNING)

data_variables = ["waterLevel","pressure","pumpSetting"]


async def dict_format(keys, values):
  return dict(zip(keys, values))

async def main():
    while True:
        url = "opc.tcp://192.168.68.132:4840/opcua/"
        async with Client(url=url) as client:
            data_list = []
            namespace = "floodDetectionOPC"
            idx = await client.get_namespace_index(namespace)
            for i in range(len(data_variables)):
                myvar = await client.nodes.root.get_child(["0:Objects", "{}:FloodDetection".format(idx), "{}:{}".format(idx,data_variables[i])])
                val = await myvar.get_value()
                data_list.append(val)
            # _list = data_list
            print(await dict_format(data_variables,data_list))
            await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(main())