import re, sys
import pymongo

class Node():
    def __init__(self, app, ri, lines, next=[]):
        self.app = app
        self.ri = ri
        self.lines = lines
        self.next = next

class LogServer():
    def __init__(self):
        pass

    def dealStream(self, line, date, hour, app, host):
        file_name = "/log/{}/{}/{}/{}.log".format(date, hour, app, host)
        with open(file_name, 'a+') as f:
            f.write(line)
        splits = line.split()[-1].split('&')
        if splits[0].split('=')[0] == 'corr_id_':
            client = pymongo.MongoClient()
            collection = client.prime.corr_id_
            item = {
                'corr_id_': splits[0].split('=')[1],
                'file_path': file_name,
            }
            collection.insert(item)
        return


    def searchDatabase(self, corr_id):
        client = pymongo.MongoClient()
        collection = client.prime.corr_id_
        result = collection.find({'corr_id_': corr_id})
        result = list(set([item['file_path'] for item in result]))
        return result


    def filterLines(self, file, corr_id):
        with open(file, 'r') as f:
            lines = f.readlines()
        ix = 0
        length = len(lines)
        ret = []
        start, end = -1, -1
        ris = []
        while ix < length:
            if lines[ix][0] == 't':
                start = ix
                ris = []
            elif lines[ix][0] == 'T':
                end = ix + 1
                cur_corr_id = self._retrieve_attributes(lines[ix], 'corr_id_')
                if cur_corr_id == corr_id:
                    cur_ri = self._retrieve_attributes(lines[ix], 'ri')
                    item = {
                        'part': lines[start:end],
                        'ris': ris,
                        'ri': cur_ri
                    }
                    ret.append(item)
            else:
                cur_corr_id = self._retrieve_attributes(lines[ix], 'corr_id_')
                if cur_corr_id and cur_corr_id == corr_id:
                    ris.append(self._retrieve_attributes(lines[ix], 'ri'))
            ix += 1
        return ret


    def _retrieve_attributes(self, str, attr):
        """
        :param str:
        :param attr:
        :return:
            None if there is no attribute string
            "" if there is attribute string but no attr
            string if there exists attr's value
        """
        splits = str.split()[-1].split('&')
        if splits is None:
            return None
        for item in splits:
            if item.split('=')[0] == attr:
                return item.split('=')[1]
        return ""


    def search(self, corr_id_):
        files = self.searchDatabase(corr_id_)
        ri2part = {}
        ri2app = {}
        ri2ris = {}
        root_app = None

        for file in files:
            # part_info is a list of part, ri, ris
            part_info = self.filterLines(file, corr_id_)
            app = re.match(".*/Application_(.*)/", file).groups()[0]
            for info in part_info:
                tri = info["ri"]
                ri2part[tri] = info["part"]
                ri2app[tri] = app
                ri2ris[tri] = ri2ris.setdefault(tri, []) + info["ris"]
                if tri == "":
                    root_app = app
        root = Node(root_app, "", ri2part[""])

        def _search(rt):
            for tri in ri2ris.get(rt.ri, []):
                to_append = Node(tri, ri2app[tri], ri2part[tri])
                rt.next.append(_search(to_append))
            return rt

        root = _search(root)
        return root

