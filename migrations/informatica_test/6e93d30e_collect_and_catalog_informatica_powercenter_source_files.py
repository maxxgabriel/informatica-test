import os
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import col, lit, current_timestamp, explode, count, size
from datetime import datetime
import json
import glob
import hashlib

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Informatica_PowerCenter_Artifact_Collector") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configuration
POWERCENTRE_XML_PATH = "/path/to/powercentre/exports"
OUTPUT_BASE_PATH = "/path/to/migration/artifacts"
INVENTORY_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/inventory"
CATALOG_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/catalog"

# Define schemas for artifact inventory
workflow_schema = StructType([
    StructField("workflow_name", StringType(), False),
    StructField("workflow_version", StringType(), True),
    StructField("workflow_description", StringType(), True),
    StructField("folder_name", StringType(), True),
    StructField("is_valid", StringType(), True),
    StructField("scheduler_enabled", StringType(), True),
    StructField("workflow_xml_path", StringType(), True),
    StructField("session_count", IntegerType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("file_checksum", StringType(), True)
])

mapping_schema = StructType([
    StructField("mapping_name", StringType(), False),
    StructField("mapping_version", StringType(), True),
    StructField("mapping_description", StringType(), True),
    StructField("folder_name", StringType(), True),
    StructField("is_valid", StringType(), True),
    StructField("source_count", IntegerType(), True),
    StructField("target_count", IntegerType(), True),
    StructField("transformation_count", IntegerType(), True),
    StructField("mapping_xml_path", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("file_checksum", StringType(), True)
])

session_schema = StructType([
    StructField("session_name", StringType(), False),
    StructField("session_description", StringType(), True),
    StructField("workflow_name", StringType(), True),
    StructField("mapping_name", StringType(), True),
    StructField("folder_name", StringType(), True),
    StructField("is_valid", StringType(), True),
    StructField("session_type", StringType(), True),
    StructField("session_xml_path", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("file_checksum", StringType(), True)
])

transformation_schema = StructType([
    StructField("transformation_name", StringType(), False),
    StructField("transformation_type", StringType(), True),
    StructField("transformation_description", StringType(), True),
    StructField("mapping_name", StringType(), True),
    StructField("folder_name", StringType(), True),
    StructField("is_reusable", StringType(), True),
    StructField("port_count", IntegerType(), True),
    StructField("expression_logic", StringType(), True),
    StructField("transformation_xml_path", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("file_checksum", StringType(), True)
])

source_target_mapping_schema = StructType([
    StructField("mapping_name", StringType(), False),
    StructField("source_name", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("source_database", StringType(), True),
    StructField("source_owner", StringType(), True),
    StructField("target_name", StringType(), True),
    StructField("target_type", StringType(), True),
    StructField("target_database", StringType(), True),
    StructField("target_owner", StringType(), True),
    StructField("folder_name", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True)
])

parameter_file_schema = StructType([
    StructField("parameter_file_name", StringType(), False),
    StructField("parameter_type", StringType(), True),
    StructField("workflow_name", StringType(), True),
    StructField("session_name", StringType(), True),
    StructField("parameter_key", StringType(), True),
    StructField("parameter_value", StringType(), True),
    StructField("parameter_file_path", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("file_checksum", StringType(), True)
])


def calculate_file_checksum(file_path):
    """
    Calculate MD5 checksum for file integrity validation.
    
    Args:
        file_path: Path to the file
        
    Returns:
        MD5 checksum string
    """
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        return f"ERROR: {str(e)}"


def parse_xml_safe(xml_path):
    """
    Safely parse XML file with error handling.
    
    Args:
        xml_path: Path to XML file
        
    Returns:
        ElementTree root or None if parsing fails
    """
    try:
        tree = ET.parse(xml_path)
        return tree.getroot()
    except Exception as e:
        print(f"Error parsing XML {xml_path}: {str(e)}")
        return None


def extract_workflows(xml_files):
    """
    Extract workflow information from PowerCenter XML exports.
    
    Args:
        xml_files: List of XML file paths
        
    Returns:
        List of workflow dictionaries
    """
    workflows = []
    
    for xml_file in xml_files:
        root = parse_xml_safe(xml_file)
        if root is None:
            continue
            
        for workflow in root.findall(".//WORKFLOW"):
            workflow_data = {
                "workflow_name": workflow.get("NAME", ""),
                "workflow_version": workflow.get("VERSIONNUMBER", ""),
                "workflow_description": workflow.get("DESCRIPTION", ""),
                "folder_name": root.find(".//FOLDER").get("NAME", "") if root.find(".//FOLDER") is not None else "",
                "is_valid": workflow.get("ISVALID", ""),
                "scheduler_enabled": workflow.get("SCHEDULERACTIVE", ""),
                "workflow_xml_path": xml_file,
                "session_count": len(workflow.findall(".//SESSION")),
                "extraction_timestamp": datetime.now(),
                "file_checksum": calculate_file_checksum(xml_file)
            }
            workflows.append(workflow_data)
    
    return workflows


def extract_mappings(xml_files):
    """
    Extract mapping information from PowerCenter XML exports.
    
    Args:
        xml_files: List of XML file paths
        
    Returns:
        List of mapping dictionaries
    """
    mappings = []
    
    for xml_file in xml_files:
        root = parse_xml_safe(xml_file)
        if root is None:
            continue
            
        for mapping in root.findall(".//MAPPING"):
            source_count = len(mapping.findall(".//SOURCE"))
            target_count = len(mapping.findall(".//TARGET"))
            transformation_count = len(mapping.findall(".//TRANSFORMATION"))
            
            mapping_data = {
                "mapping_name": mapping.get("NAME", ""),
                "mapping_version": mapping.get("VERSIONNUMBER", ""),
                "mapping_description": mapping.get("DESCRIPTION", ""),
                "folder_name": root.find(".//FOLDER").get("NAME", "") if root.find(".//FOLDER") is not None else "",
                "is_valid": mapping.get("ISVALID", ""),
                "source_count": source_count,
                "target_count": target_count,
                "transformation_count": transformation_count,
                "mapping_xml_path": xml_file,
                "extraction_timestamp": datetime.now(),
                "file_checksum": calculate_file_checksum(xml_file)
            }
            mappings.append(mapping_data)
    
    return mappings


def extract_sessions(xml_files):
    """
    Extract session information from PowerCenter XML exports.
    
    Args:
        xml_files: List of XML file paths
        
    Returns:
        List of session dictionaries
    """
    sessions = []
    
    for xml_file in xml_files:
        root = parse_xml_safe(xml_file)
        if root is None:
            continue
            
        for session in root.findall(".//SESSION"):
            session_data = {
                "session_name": session.get("NAME", ""),
                "session_description": session.get("DESCRIPTION", ""),
                "workflow_name": root.find(".//WORKFLOW").get("NAME", "") if root.find(".//WORKFLOW") is not None else "",
                "mapping_name": session.get("MAPPINGNAME", ""),
                "folder_name": root.find(".//FOLDER").get("NAME", "") if root.find(".//FOLDER") is not None else "",
                "is_valid": session.get("ISVALID", ""),
                "session_type": session.get("TYPE", ""),
                "session_xml_path": xml_file,
                "extraction_timestamp": datetime.now(),
                "file_checksum": calculate_file_checksum(xml_file)
            }
            sessions.append(session_data)
    
    return sessions


def extract_transformations(xml_files):
    """
    Extract transformation information from PowerCenter XML exports.
    
    Args:
        xml_files: List of XML file paths
        
    Returns:
        List of transformation dictionaries
    """
    transformations = []
    
    for xml_file in xml_files:
        root = parse_xml_safe(xml_file)
        if root is None:
            continue
            
        mapping_name = ""
        mapping_elem = root.find(".//MAPPING")
        if mapping_elem is not None:
            mapping_name = mapping_elem.get("NAME", "")
        
        folder_name = ""
        folder_elem = root.find(".//FOLDER")
        if folder_elem is not None:
            folder_name = folder_elem.get("NAME", "")
            
        for transformation in root.findall(".//TRANSFORMATION"):
            port_count = len(transformation.findall(".//TRANSFORMFIELD"))
            
            expression_logic = ""
            if transformation.get("TYPE") == "Expression":
                expressions = transformation.findall(".//TRANSFORMFIELD[@EXPRESSION]")
                expression_logic = "; ".join([expr.get("EXPRESSION", "") for expr in expressions])
            
            transformation_data = {
                "transformation_name": transformation.get("NAME", ""),
                "transformation_type": transformation.get("TYPE", ""),
                "transformation_description": transformation.get("DESCRIPTION", ""),
                "mapping_name": mapping_name,
                "folder_name": folder_name,
                "is_reusable": transformation.get("REUSABLE", "NO"),
                "port_count": port_count,
                "expression_logic": expression_logic[:1000] if expression_logic else "",
                "transformation_xml_path": xml_file,
                "extraction_timestamp": datetime.now(),
                "file_checksum": calculate_file_checksum(xml_file)
            }
            transformations.append(transformation_data)
    
    return transformations


def extract_source_target_mappings(xml_files):
    """
    Extract source-to-target mapping relationships.
    
    Args:
        xml_files: List of XML file paths
        
    Returns:
        List of source-target mapping dictionaries
    """
    source_target_mappings = []
    
    for xml_file in xml_files:
        root = parse_xml_safe(xml_file)
        if root is None:
            continue
            
        for mapping in root.findall(".//MAPPING"):
            mapping_name = mapping.get("NAME", "")
            folder_name = root.find(".//FOLDER").get("NAME", "") if root.find(".//FOLDER") is not None else ""
            
            sources = mapping.findall(".//SOURCE")
            targets = mapping.findall(".//TARGET")
            
            for source in sources:
                for target in targets:
                    source_qualifier = source.find(".//SOURCEFIELD")
                    target_field = target.find(".//TARGETFIELD")
                    
                    mapping_data = {
                        "mapping_name": mapping_name,
                        "source_name": source.get("NAME", ""),
                        "source_type": source.get("OBJECTTYPE", ""),
                        "source_database": source.get("DATABASETYPE", ""),
                        "source_owner": source.get("OWNERNAME", ""),
                        "target_name": target.get("NAME", ""),
                        "target_type": target.get("OBJECTTYPE", ""),
                        "target_database": target.get("DATABASETYPE", ""),
                        "target_owner": target.get("OWNERNAME", ""),
                        "folder_name": folder_name,
                        "extraction_timestamp": datetime.now()
                    }
                    source_target_mappings.append(mapping_data)
    
    return source_target_mappings


def extract_parameter_files(param_file_paths, xml_files):
    """
    Extract parameter file information and parse parameter values.
    
    Args:
        param_file_paths: List of parameter file paths
        xml_files: List of XML file paths for context
        
    Returns:
        List of parameter file dictionaries
    """
    parameters = []
    
    for param_file in param_file_paths:
        try:
            with open(param_file, 'r') as f:
                lines = f.readlines()
                
            current_section = ""
            workflow_name = ""
            session_name = ""
            
            for line in lines:
                line = line.strip()
                
                if line.startswith("[") and line.endswith("]"):
                    current_section = line[1:-1]
                    if "." in current_section:
                        parts = current_section.split(".")
                        workflow_name = parts[0] if len(parts) > 0 else ""
                        session_name = parts[1] if len(parts) > 1 else ""
                elif "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    
                    param_data = {
                        "parameter_file_name": os.path.basename(param_file),
                        "parameter_type": "SESSION" if session_name else "WORKFLOW",
                        "workflow_name": workflow_name,
                        "session_name": session_name,
                        "parameter_key": key.strip(),
                        "parameter_value": value.strip(),
                        "parameter_file_path": param_file,
                        "extraction_timestamp": datetime.now(),
                        "file_checksum": calculate_file_checksum(param_file)
                    }
                    parameters.append(param_data)
        except Exception as e:
            print(f"Error parsing parameter file {param_file}: {str(e)}")
    
    return parameters


def organize_artifacts(xml_files, param_files):
    """
    Organize collected artifacts into structured folder hierarchy.
    
    Args:
        xml_files: List of XML file paths
        param_files: List of parameter file paths
        
    Returns:
        Dictionary with organized file paths
    """
    organized_structure = {
        "workflows": f"{OUTPUT_BASE_PATH}/workflows",
        "mappings": f"{OUTPUT_BASE_PATH}/mappings",
        "sessions": f"{OUTPUT_BASE_PATH}/sessions",
        "transformations": f"{OUTPUT_BASE_PATH}/transformations",
        "parameters": f"{OUTPUT_BASE_PATH}/parameters",
        "source_target_mappings": f"{OUTPUT_BASE_PATH}/source_target_mappings"
    }
    
    for folder_path in organized_structure.values():
        os.makedirs(folder_path, exist_ok=True)
    
    return organized_structure


def create_inventory_summary(workflows_df, mappings_df, sessions_df, transformations_df, parameters_df):
    """
    Create comprehensive inventory summary with counts and descriptions.
    
    Args:
        workflows_df: Workflows DataFrame
        mappings_df: Mappings DataFrame
        sessions_df: Sessions DataFrame
        transformations_df: Transformations DataFrame
        parameters_df: Parameters DataFrame
        
    Returns:
        Summary DataFrame
    """
    summary_data = [
        {
            "artifact_type": "Workflows",
            "total_count": workflows_df.count(),
            "unique_folders": workflows_df.select("folder_name").distinct().count(),
            "extraction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "Collected"
        },
        {
            "artifact_type": "Mappings",
            "total_count": mappings_df.count(),
            "unique_folders": mappings_df.select("folder_name").distinct().count(),
            "extraction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "Collected"
        },
        {
            "artifact_type": "Sessions",
            "total_count": sessions_df.count(),
            "unique_folders": sessions_df.select("folder_name").distinct().count(),
            "extraction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "Collected"
        },
        {
            "artifact_type": "Transformations",
            "total_count": transformations_df.count(),
            "unique_folders": transformations_df.select("folder_name").distinct().count(),
            "extraction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "Collected"
        },
        {
            "artifact_type": "Parameter Files",
            "total_count": parameters_df.count(),
            "unique_folders": parameters_df.select("workflow_name").distinct().count(),
            "extraction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "Collected"
        }
    ]
    
    summary_df = spark.createDataFrame(summary_data)
    return summary_df


def create_transformation_type_summary(transformations_df):
    """
    Create summary of transformation types and counts.
    
    Args:
        transformations_df: Transformations DataFrame
        
    Returns:
        Transformation type summary DataFrame
    """
    transformation_summary = transformations_df \
        .groupBy("transformation_type") \
        .agg(
            count("*").alias("count"),
            count(col("is_reusable").when(col("is_reusable") == "YES", 1)).alias("reusable_count")
        ) \
        .orderBy(col("count").desc())
    
    return transformation_summary


def create_folder_hierarchy_catalog(workflows_df, mappings_df, sessions_df):
    """
    Create folder hierarchy catalog showing repository structure.
    
    Args:
        workflows_df: Workflows DataFrame
        mappings_df: Mappings DataFrame
        sessions_df: Sessions DataFrame
        
    Returns:
        Folder hierarchy DataFrame
    """
    folder_hierarchy = workflows_df \
        .select("folder_name") \
        .union(mappings_df.select("folder_name")) \
        .union(sessions_df.select("folder_name")) \
        .distinct() \
        .withColumn("workflow_count", lit(0)) \
        .withColumn("mapping_count", lit(0)) \
        .withColumn("session_count", lit(0))
    
    workflow_counts = workflows_df.groupBy("folder_name").agg(count("*").alias("workflow_count"))
    mapping_counts = mappings_df.groupBy("folder_name").agg(count("*").alias("mapping_count"))
    session_counts = sessions_df.groupBy("folder_name").agg(count("*").alias("session_count"))
    
    folder_hierarchy = folder_hierarchy \
        .join(workflow_counts, "folder_name", "left") \
        .join(mapping_counts, "folder_name", "left") \
        .join(session_counts, "folder_name", "left")
    
    return folder_hierarchy


def main():
    """
    Main execution function to collect and catalog Informatica PowerCenter artifacts.
    """
    print("=" * 80)
    print("Informatica PowerCenter Artifact Collection and Cataloging")
    print("=" * 80)
    
    # Create output directories
    os.makedirs(OUTPUT_BASE_PATH, exist_ok=True)
    os.makedirs(INVENTORY_OUTPUT_PATH, exist_ok=True)
    os.makedirs(CATALOG_OUTPUT_PATH, exist_ok=True)
    
    # Collect XML files
    print("\n[Step 1] Collecting XML export files...")
    xml_files = glob.glob(f"{POWERCENTRE_XML_PATH}/**/*.xml", recursive=True) + \
                glob.glob(f"{POWERCENTRE_XML_PATH}/**/*.XML", recursive=True)
    print(f"Found {len(xml_files)} XML files")
    
    # Collect parameter files
    print("\n[Step 2] Collecting parameter files...")
    param_files = glob.glob(f"{POWERCENTRE_XML_PATH}/**/*.txt", recursive=True) + \
                  glob.glob(f"{POWERCENTRE_XML_PATH}/**/*.param", recursive=True)
    print(f"Found {len(param_files)} parameter files")
    
    # Extract workflows
    print("\n[Step 3] Extracting workflow definitions...")
    workflows_list = extract_workflows(xml_files)
    workflows_df = spark.createDataFrame(workflows_list, workflow_schema)
    print(f"Extracted {workflows_df.count()} workflows")
    
    # Extract mappings
    print("\n[Step 4] Extracting mapping definitions...")
    mappings_list = extract_mappings(xml_files)
    mappings_df = spark.createDataFrame(mappings_list, mapping_schema)
    print(f"Extracted {mappings_df.count()} mappings")
    
    # Extract sessions
    print("\n[Step 5] Extracting session definitions...")
    sessions_list = extract_sessions(xml_files)
    sessions_df = spark.createDataFrame(sessions_list, session_schema)
    print(f"Extracted {sessions_df.count()} sessions")
    
    # Extract transformations
    print("\n[Step 6] Extracting transformation definitions...")
    transformations_list = extract_transformations(xml_files)
    transformations_df = spark.createDataFrame(transformations_list, transformation_schema)
    print(f"Extracted {transformations_df.count()} transformations")
    
    # Extract source-to-target mappings
    print("\n[Step 7] Extracting source-to-target mappings...")
    source_target_list = extract_source_target_mappings(xml_files)
    source_target_df = spark.createDataFrame(source_target_list, source_target_mapping_schema)
    print(f"Extracted {source_target_df.count()} source-to-target mappings")
    
    # Extract parameter files
    print("\n[Step 8] Extracting parameter file contents...")
    parameters_list = extract_parameter_files(param_files, xml_files)
    parameters_df = spark.createDataFrame(parameters_list, parameter_file_schema)
    print(f"Extracted {parameters_df.count()} parameter entries")
    
    # Organize artifacts
    print("\n[Step 9] Organizing artifacts into folder structure...")
    organized_structure = organize_artifacts(xml_files, param_files)
    
    # Save workflows inventory
    workflows_df.write.mode("overwrite").parquet(f"{organized_structure['workflows']}/workflows_inventory.parquet")
    workflows_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{organized_structure['workflows']}/workflows_inventory.csv")
    
    # Save mappings inventory
    mappings_df.write.mode("overwrite").parquet(f"{organized_structure['mappings']}/mappings_inventory.parquet")
    mappings_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{organized_structure['mappings']}/mappings_inventory.csv")
    
    # Save sessions inventory
    sessions_df.write.mode("overwrite").parquet(f"{organized_structure['sessions']}/sessions_inventory.parquet")
    sessions_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{organized_structure['sessions']}/sessions_inventory.csv")
    
    # Save transformations inventory
    transformations_df.write.mode("overwrite").parquet(f"{organized_structure['transformations']}/transformations_inventory.parquet")
    transformations_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{organized_structure['transformations']}/transformations_inventory.csv")
    
    # Save source-to-target mappings
    source_target_df.write.mode("overwrite").parquet(f"{organized_structure['source_target_mappings']}/source_target_mappings.parquet")
    source_target_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{organized_structure['source_target_mappings']}/source_target_mappings.csv")
    
    # Save parameters
    parameters_df.write.mode("overwrite").parquet(f"{organized_structure['parameters']}/parameters_inventory.parquet")
    parameters_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{organized_structure['parameters']}/parameters_inventory.csv")
    
    # Create inventory summary
    print("\n[Step 10] Creating inventory summary...")
    inventory_summary_df = create_inventory_summary(
        workflows_df, mappings_df, sessions_df, transformations_df, parameters_df
    )
    inventory_summary_df.write.mode("overwrite").parquet(f"{INVENTORY_OUTPUT_PATH}/inventory_summary.parquet")
    inventory_summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{INVENTORY_OUTPUT_PATH}/inventory_summary.csv")
    
    # Create transformation type summary
    print("\n[Step 11] Creating transformation type summary...")
    transformation_summary_df = create_transformation_type_summary(transformations_df)
    transformation_summary_df.write.mode("overwrite").parquet(f"{CATALOG_OUTPUT_PATH}/transformation_type_summary.parquet")
    transformation_summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CATALOG_OUTPUT_PATH}/transformation_type_summary.csv")
    
    # Create folder hierarchy catalog
    print("\n[Step 12] Creating folder hierarchy catalog...")
    folder_hierarchy_df = create_folder_hierarchy_catalog(workflows_df, mappings_df, sessions_df)
    folder_hierarchy_df.write.mode("overwrite").parquet(f"{CATALOG_OUTPUT_PATH}/folder_hierarchy_catalog.parquet")
    folder_hierarchy_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CATALOG_OUTPUT_PATH}/folder_hierarchy_catalog.csv")
    
    # Display summary statistics
    print("\n" + "=" * 80)
    print("COLLECTION AND CATALOGING SUMMARY")
    print("=" * 80)
    print(f"\nTotal Workflows: {workflows_df.count()}")
    print(f"Total Mappings: {mappings_df.count()}")
    print(f"Total Sessions: {sessions_df.count()}")
    print(f"Total Transformations: {transformations_df.count()}")
    print(f"Total Source-Target Mappings: {source_target_df.count()}")
    print(f"Total Parameter Entries: {parameters_df.count()}")
    print(f"\nUnique Folders: {workflows_df.select('folder_name').distinct().count()}")
    
    print("\n[Step 13] Displaying transformation type breakdown...")
    transformation_summary_df.show(truncate=False)
    
    print("\n[Step 14] Displaying folder hierarchy...")
    folder_hierarchy_df.orderBy("folder_name").show(truncate=False)
    
    print("\n" + "=" * 80)
    print("ARTIFACT COLLECTION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"\nInventory Location: {INVENTORY_OUTPUT_PATH}")
    print(f"Catalog Location: {CATALOG_OUTPUT_PATH}")
    print(f"Organized Artifacts: {OUTPUT_BASE_PATH}")
    print("\nAll artifacts have been collected, cataloged, and organized.")
    print("=" * 80)


if __name__ == "__main__":
    main()
    spark.stop()