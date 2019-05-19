import socket
import bitstring
from pymongo import MongoClient
import datetime

client = MongoClient()
db = client['riw_db']

class DNS_Client:
    def __to_hex_string__(self, x):
        result = "0"

        if x.__class__.__name__ == "int" and x >= 0:
            result = hex(x)

        if x.__class__.__name__ == "int" and x < 16:
            result = "0" + result[2:]
        elif x.__class__.__name__ == "str":
            result = "".join([hex(ord(y))[2:] for y in x])

        return "0x" + result

    def __send_udp_message__(self, message, address, port):
        server_address = (address, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            sock.sendto(message.tobytes(), server_address)
            data, _ = sock.recvfrom(512)
        finally:
            sock.close()

        return bitstring.BitArray(bytes=data)

    def __format_hex__(self, hex_num):
        octets = [hex_num[i:i+2] for i in range(0, len(hex_num), 2)]
        pairs = [" ".join(octets[i:i+2]) for i in range(0, len(octets), 2)]
        return "\n".join(pairs)

    def __create_request_msg__(self, url):

        url = url.split(".")

        DNS_QUERY_FORMAT = [
            "hex=id",
            "bin=flags",
            "uintbe:16=qdcount",
            "uintbe:16=ancount",
            "uintbe:16=nscount",
            "uintbe:16=arcount"
        ]

        DNS_QUERY = {
            "id": "0x1a2b",
            "flags": "0b0000000100000000",
            "qdcount": 1,
            "ancount": 0,
            "nscount": 0,
            "arcount": 0
        }

        j = 0
        # url to bytes
        for i, _ in enumerate(url):

            url[i] = url[i].strip()

            DNS_QUERY_FORMAT.append("hex=" + "qname" + str(j))

            DNS_QUERY["qname" + str(j)] = self.__to_hex_string__(len(url[i]))

            j += 1

            DNS_QUERY_FORMAT.append("hex=" + "qname" + str(j))

            DNS_QUERY["qname" + str(j)] = self.__to_hex_string__(url[i])

            j += 1

        DNS_QUERY_FORMAT.append("hex=qname" + str(j))
        DNS_QUERY["qname" + str(j)] = self.__to_hex_string__(0)
        DNS_QUERY_FORMAT.append("uintbe:16=qtype")
        DNS_QUERY["qtype"] = 1
        DNS_QUERY_FORMAT.append("uintbe:16=qclass")
        DNS_QUERY["qclass"] = 1

        data = bitstring.pack(",".join(DNS_QUERY_FORMAT), **DNS_QUERY)
        return data

    def get_ip(self, domain):
        message = self.__create_request_msg__(domain)
        response = self.__send_udp_message__(message, "8.8.8.8", 53)

        ip_address = ".".join([
            str(response[-32:-24].uintbe),
            str(response[-24:-16].uintbe),
            str(response[-16:-8].uintbe),
            str(response[-8:].uintbe)
        ])

        # add to cache new domain
        ttl = response[-80:-48].uintbe
        dns_cache_coll = db['dns_cache']
        dns_cache_coll.insert_one({'domain': domain, 'ip_address': ip_address, 'ttl': ttl, 'insert_time': datetime.datetime.utcnow()})

        return ip_address

    def check_cache(self, domain):
        dns_cache_coll = db['dns_cache']
        domain_record = dns_cache_coll.find_one({'domain': domain})

        # diff of 2 'datime.utfnow()' objects returns a 'timedelta' object
        if not domain_record:
            return None
        elif (datetime.datetime.utcnow() - domain_record['insert_time']).seconds <= domain_record['ttl']:
            return domain_record['ip_address']
        else:
            dns_cache_coll.remove({'_id': domain_record['_id']})
            return None


DNS_CLIENT = DNS_Client()

if __name__ == "__main__":
    print(DNS_CLIENT.get_ip("riweb.tibeica.com"))
