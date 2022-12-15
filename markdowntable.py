import re


class MarkdownTable:
    def __init__(self, headers):
        assert isinstance(headers, list), 'Header has to be an array, dummy'
        self.header_count = len(headers)
        self.headers = headers
        self.current_row = {}
        self.initializeCurrentRow()
        self.table = self.makeHeaders(self.headers)
        self.rows = 0

    def initializeCurrentRow(self):
        for h in self.headers:
            self.current_row[str(h)] = None

    def makeHeaders(self, header):
        table_header = ""
        for i, h in enumerate(header):
            table_header += h
            if i < len(header):
                table_header += ","
        table_header += "\n"
        return table_header

    def addRow(self, new_row_fields):
        assert len(
            new_row_fields) == self.header_count, 'New row has to have the same number of columns as header'
        new_row = ""
        for i, f in enumerate(new_row_fields):
            if isinstance(f, int):
                new_row += str(f)
            else:
                if "\n" in f:
                    new_row += self.cleanNewlinesForCSV(f)
                else:
                    if isinstance(f, list):
                        safe = "\"" + ', '.join(f) + "\""
                        new_row += safe
                    else:
                        new_row += f
            if i < len(new_row_fields):
                new_row += ","
        new_row += "\n"
        self.table += new_row
        self.rows += 1

    def addRowRefactored(self, new_row_fields):
        # assert(len(new_row_fields.keys(
        # ))) == self.header_count, 'New row has to have the same number of columns as header'
        for field in new_row_fields.keys():
            value = new_row_fields[field]
            safe_value = ''
            if isinstance(value, int):
                safe_value = str(value)
            else:
                if "\n" in str(value):
                    safe_value = self.cleanNewlinesForCSV(value)
                elif isinstance(value, list):
                    safe_value = "\"" + ', '.join(map(str, value)) + "\""
                else:
                    safe_value = str(value)
            self.current_row[field] = safe_value
        self.table += self.row_to_csv(self.current_row)
        self.table += "\n"
        self.rows += 1

    def row_to_csv(self, row):
        new_row = ''
        for i, field in enumerate(self.headers):
            new_row += row[field]
            if i < len(self.headers):
                new_row += ","
        return new_row

    def cleanNewlinesForCSV(self, dirty_text):
        return "\"" + dirty_text + "\""

    def printTable(self):
        print(self.table)

    def getTable(self):
        return self.table

    def purgeTable(self):
        self.table = self.makeHeaders(self.headers)
        self.initializeCurrentRow()
        self.rows = 0

    def makeTable(self, outname):
        assert outname[-4:
                       ] == '.csv', 'Outname has to be a csv file (.csv), my guy'
        with open(outname, 'w') as file:
            file.write(self.table)
        print(f"CSV table written to {outname}")
        file.close()
