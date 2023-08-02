

class Dataset:
    def __init__(self, values: dict = None):
        values = values if values is not None else {}
        self.identifier: str = values.get("identifier", None)
        self.title: str = values.get("title", None)
        self.description: str = values.get("description", None)
        self.subject: str = values.get("subject", None)
        self.coverage: str = values.get("coverage", None)
        self.source: str = values.get("source", None)
        self.creator: str = values.get("creator", None)
        self.publisher: str = values.get("publisher", None)
        self.date: str = values.get("date", None)
        self.format: str = values.get("format", None)
        self.type: str = values.get("type", None)
        self.contributor: str = values.get("contributor", None)
        self.language: str = values.get("language", None)
        self.relation: str = values.get("relation", None)
        self.rights: str = values.get("rights", None)
        self.license: str = values.get("license", None)
        self.url: str = values.get("url", None)
