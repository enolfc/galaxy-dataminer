import json

from six.moves.html_parser import HTMLParser


class CallerHTMLParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        if tag == "script":
            for attr in attrs:
                if attr[0] == "id" and attr[1] == "dataminer-output":
                    self._caller_dataminer_script = True

    def handle_endtag(self, tag):
        self._caller_dataminer_script = False

    def handle_data(self, data):
        if getattr(self, "_caller_dataminer_script", False):
            self._caller_dataminer_data = json.loads(data)

    def caller_dataminer_data(self):
        return getattr(self, "_caller_dataminer_data", None)
