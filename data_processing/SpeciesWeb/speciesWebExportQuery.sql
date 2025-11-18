USE dassco_zxing_prod;

SELECT 
    s.barcode,
    s.id AS specimen_id,
    s.guid,
    s.digitiser,
    s.date_asset_taken,
    s.folder_id,
    f.id AS folder_version_id,
    f.area,
    f.family AS family_speciesweb,
    f.genus AS genus_speciesweb,
    f.species AS species_speciesweb,
    f.variety AS variety_speciesweb,
    f.subsp AS subspecies_speciesweb,
    f.highest_classification AS lowest_classification_speciesweb,
    f.gbif_match_json,
    f.created_at,
    folders.approved_at
FROM specimen s
LEFT JOIN (
    SELECT fv.*
    FROM folder_versions fv
    INNER JOIN (
        SELECT folder_id, MAX(created_at) AS max_created_at
        FROM folder_versions
        GROUP BY folder_id
    ) AS max_fv 
    ON fv.folder_id = max_fv.folder_id AND fv.created_at = max_fv.max_created_at
) f ON s.folder_id = f.folder_id
JOIN folders ON s.folder_id = folders.id
WHERE folders.approved_at > '2025-08-13' LIMIT 100000;