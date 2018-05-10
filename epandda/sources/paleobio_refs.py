class paleobio_refs:
    def dbName(self):
        return "test"

    def collectionName(self):
        return "pbdb_refs"

    def availableFields(self):
        return ["reference_no", "genus", "accepted_name", "family", "order", "class", "country", "state", "cc1", "phylum", "collection_comments", "collectors",
                "collection_type", "preservation_comments", "stratcomments", "geogcomments", "collection_dates", "county", "identified_name", "museum",
                "occ_refs-author1init", "occ_refs-author1last", "occ_refs-author2init", "occ_refs-author2last", "occ_refs-doi", "occ_refs-editors", "occ_refs-otherauthors",
                "occ_refs-publication_type", "occ_refs-pubtitle", "occ_refs-pubvol", "occ_refs-pubyr", "occ_refs-reftitle", "occ_refs-formatted", "ref_author"]
