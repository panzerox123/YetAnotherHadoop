from io import SEEK_SET, FileIO


with open("test.txt", 'a') as f:
            f.seek(0)
            f.seek(10)
            print(f.tell())
            f.write("THE NEW GODDAMN DATA")
            f.close()