class paleobio:
    def dbName(self):
        return "test"

    def collectionName(self):
        return "pbdb_flat_index"

    def availableFields(self):
        return ["occurrence_no", "genus_name", "species_reso", "species_name", "subgenus_reso", "subgenus_name", 
"abund_value", "abund_unit", "reference_no" "occ_reference_no",
"comments", "taxon_no", "country", "state", "county", "collection_aka", "collection_name", "research_group",
"period_min", "period_max", "emlperiod_max", "emlperiod_min", "emlepoch_max", "emlepoch_min", 
"epoch_max", "epoch_min", "emlintage_min", "emlintage_max", "intage_min", "intage_max", 
"emllocage_min", "emllocage_max", "locage_min", "locage_max", "zone_type", "zone", "formation", 
"member", "lithadj", "lithification", "minor_lithology", "lithology1", "lithology2", "lithadj2", 
"lithification2", "minor_lithology2", "environment", "tectonic_setting", "geology_comments", 
"taxonomy_comments", "collection_comments", "collectors", "collection_type", "assembl_comps", 
"preservation_comments", "pres_mode", "lithdescript", "stratcomments", "stratscale", "geogcomments", 
"paleolng", "paleolat", "plate", "latlng_precision", "lat", "lng", "reference_no" "coll_reference_no", "collection_no"]
