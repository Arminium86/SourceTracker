# Import related models for typing help (optional)
from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle # type: ignore
input_set: SessionModelSet = input_set # type: ignore
output_set: SessionModelSet = output_set # type: ignore

from libblocksmith import BlockModel  # type: ignore
from workflow.execution.tags import tag
from xml.sax.saxutils import escape
import os
import pandas as pd


def desktop_path(fallback_dir: str) -> str:
    d = os.path.join(os.path.expanduser("~"), "Desktop")
    return d if os.path.isdir(d) else fallback_dir


def build_field_entries_from_df(df: pd.DataFrame) -> str:
    if "Structure_FullName" not in df.columns:
        raise ValueError("Expected column 'Structure_FullName' in the input model.")

    unique_structures = sorted(df["Structure_FullName"].dropna().unique())

    created_hierarchy = set()
    field_entries = []

    # ---- ADD ROOT NODE FIRST ----
    root_entry = """
    <Field>
      <Indentation>0</Indentation>
      <Name>SourceTracker</Name>
      <Description>SourceTracker</Description>
      <Units />
      <SummaryType>None</SummaryType>
      <FieldType>Title</FieldType>
      <WeightingFieldIndex>-1</WeightingFieldIndex>
      <Format />
      <ReadOnly>true</ReadOnly>
      <IncludeInProduct>false</IncludeInProduct>
    </Field>
    """.strip()

    field_entries.append(root_entry)

    # ---- THEN BUILD CHILD STRUCTURE ----
    for structure in unique_structures:
        parts = str(structure).split("/")
        current_path = []

        for i, part in enumerate(parts):
            indent = i + 1  # children now start at 1 (under SourceTracker)
            current_path.append(part)
            path_key = "/".join(current_path)

            if path_key in created_hierarchy:
                continue

            created_hierarchy.add(path_key)

            is_leaf = (i == len(parts) - 1)
            field_type = "Double" if is_leaf else "Title"
            summary_type = "Sum" if is_leaf else "None"

            field_entries.append(
                f"""
    <Field>
      <Indentation>{indent}</Indentation>
      <Name>{escape(part)}</Name>
      <Description>{escape(part)}</Description>
      <Units />
      <SummaryType>{summary_type}</SummaryType>
      <FieldType>{field_type}</FieldType>
      <WeightingFieldIndex>-1</WeightingFieldIndex>
      <Format />
      <ReadOnly>true</ReadOnly>
      <IncludeInProduct>false</IncludeInProduct>
    </Field>""".strip()
            )

    return "\n\n".join(field_entries)

def append_fields_to_template(template_xml: str, field_entries: str) -> str:
    """
    Inserts field_entries immediately BEFORE the closing </Fields>.
    """
    closing_tag = "</Fields>"
    idx = template_xml.rfind(closing_tag)
    if idx == -1:
        raise ValueError("Template file does not contain a </Fields> tag to append into.")

    prefix = template_xml[:idx].rstrip()
    suffix = template_xml[idx:]  # includes </Fields> ... </OpenFields>

    # Keep indentation similar to template (your template uses 2 spaces before </Fields>)
    return prefix + "\n\n" + field_entries + "\n  " + suffix.lstrip()


# -------------------------------
# Blocksmith input pattern (same as your example)
# -------------------------------
for input_model in input_set.get_all("Input Models"):
    source_file_name = str(input_model.get_attribute("SourceFileName", ""))
    source_directory = str(input_model.get_attribute("SourceDirectory", "")) or os.getcwd()

    # Read tabular input from the model
    df = input_model.read().to_pandas()

    # Generate new <Field> blocks
    new_fields = build_field_entries_from_df(df)

    # Output directory: Desktop preferred
    out_dir = desktop_path(source_directory)

    # Read the existing template from the output directory
    template_filename = "fields.openfields"
    template_path = os.path.join(out_dir, template_filename)

    if not os.path.isfile(template_path):
        raise FileNotFoundError(
            f"Could not find template file '{template_filename}' in: {out_dir}\n"
            f"Expected at: {template_path}"
        )

    with open(template_path, "r", encoding="utf-8") as f:
        template_xml = f.read()

    # Append and write final file
    final_xml = append_fields_to_template(template_xml, new_fields)

    output_path = os.path.join(out_dir, "fields_appended.openfields")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_xml)

    print(f"Template read from: {template_path}")
    print(f"Appended file saved to: {output_path}")
