import arcpy
import csv
import os
import uuid
import time
import datetime
import traceback
import sys
import abc
import shutil
import re

from zipfile import ZipFile


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Enfire Toolbox"
        self.alias = "Army Geospatial Center Enfire Tools Toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [UpdateAttachmentsTool, UpdateAttachmentsZipTool]


class BaseTool(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        self.canRunInBackground = False

    @abc.abstractmethod
    def getParameterInfo(self):
        return

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        start_time = time.clock()
        success = False
        try:
            arcpy.AddMessage("******************************************************")
            arcpy.AddMessage("Beginning processing at {0}...".format(datetime.datetime.now()))

            self.tool_execute(parameters, messages)

            success = True

            return success
        except arcpy.ExecuteError:
            msgs = arcpy.GetMessage(0)
            msgs += arcpy.GetMessages(2)

            tb_info = traceback.format_exc()
            error_message = msgs + tb_info + "\n" + str(sys.exc_type) + ": " + str(sys.exc_value)
            arcpy.AddError("arcpy messages: " + error_message)

            return False

        except:
            tb_info = traceback.format_exc()
            error_message = tb_info
            arcpy.AddError("arcpy messages: " + error_message)

            return False

        finally:
            # Error condition, so return non-zero
            if not success:
                status = "failed"

            # Success, so return zero
            else:
                status = "finished successfully"

            end_time = time.clock()
            total_time = end_time - start_time
            arcpy.AddMessage("******************************************************")
            arcpy.AddMessage("Script {0} at {1} (Elapsed time: {2})\n".format(status, datetime.datetime.now(),
                                                                              datetime.timedelta(seconds=total_time)))

    @abc.abstractmethod
    def tool_execute(self, parameters, messages):
        return


class UpdateAttachmentsTool(BaseTool):
    def __init__(self):
        super(UpdateAttachmentsTool, self).__init__(self.__class__.__name__)
        self.label = "Update Attachments in a Workspace"
        self.description = "Traverses folders to update attachments for data in a workspace"

    def getParameterInfo(self):
        """Define parameter definitions"""
        param_workspace = arcpy.Parameter(displayName="Attachment Workspace", name="workspace", datatype="DEWorkspace",
                                          parameterType="Required", direction="Input")
        param_attachment_id_field = arcpy.Parameter(displayName="Attachment ID Field", name="attachment_id_field",
                                                    datatype="GPString", parameterType="Required", direction="Input")
        param_attachment_id_field.value = "GUID_PK"
        param_attachment_folder = arcpy.Parameter(displayName="Attachment Folder", name="attachment_folder",
                                                  datatype="DEFolder", parameterType="Required", direction="Input")

        params = [param_workspace, param_attachment_id_field, param_attachment_folder]
        return params

    def tool_execute(self, parameters, messages):
        workspace = parameters[0].valueAsText
        attachment_id_field_name = parameters[1].valueAsText
        attachments_folder = parameters[2].valueAsText

        self.update_attachments(workspace, attachment_id_field_name, attachments_folder)

    @staticmethod
    def build_relative_path(base_dir, compare_dir):
        if compare_dir.startswith(base_dir):
            return "." + compare_dir.replace(base_dir, '')
        return compare_dir

    @staticmethod
    def check_field_exists(dataset, field_name):
        for field in arcpy.ListFields(dataset):
            if field.name.lower() == field_name.lower():
                return True
        return False

    def update_attachments(self, workspace, attachment_id_field_name, attachments_folder):
        attachment_extensions = ["jpg", "png", "bmp", "gif"]
        attachment_path_field_name = "AttachmentPath"

        attachment_id_picture_lookup = {}

        workspace_dir = os.path.dirname(workspace)
        workspace_name = os.path.basename(workspace)

        for root_dir in os.listdir(attachments_folder):
            root_dir_path = os.path.join(attachments_folder, root_dir)
            if not os.path.isdir(root_dir_path):
                continue

            if root_dir.lower().endswith(".gdb"):
                continue

            arcpy.AddMessage("Found picture folder: {}".format(root_dir))
            attachment_folder_id = "{" + os.path.basename(root_dir).upper() + "}"

            attachments_list = []
            for path, subdir, files in os.walk(root_dir_path, topdown=True):
                for name in files:
                    split_name = name.split(".")
                    file_extension = split_name[-1].lower()
                    if file_extension in attachment_extensions:
                        relative_path = self.build_relative_path(workspace_dir, path)
                        attachment_path = os.path.join(relative_path, name)
                        arcpy.AddMessage("Found attachment for {}: {}".format(attachment_folder_id, attachment_path))
                        attachments_list.append(attachment_path)

            arcpy.AddMessage("Found {} attachment(s) for {}".format(len(attachments_list), attachment_folder_id))
            if len(attachments_list) > 0:
                attachment_id_picture_lookup[attachment_folder_id] = attachments_list

        arcpy.AddMessage("Searching workspace {} for feature classes and tables to link the attachments to".format(workspace_name))
        dataset_attachment_lookup = {}

        walk = arcpy.da.Walk(workspace, datatype=["FeatureClass", "Table"])
        for ds_path, ds_folders, ds_names in walk:
            for ds_name in ds_names:
                dataset = os.path.join(ds_path, ds_name)
                if not self.check_field_exists(dataset, attachment_id_field_name):
                    arcpy.AddMessage("Attachment ID field {} does not exist in dataset {}. Skipping".format(
                        attachment_id_field_name, ds_name))
                    continue

                arcpy.AddMessage("Looking for matching records in dataset {}".format(ds_name))
                attachments_list = []
                with arcpy.da.SearchCursor(dataset, attachment_id_field_name) as search_cursor:
                    for row in search_cursor:
                        attachment_id = row[0]
                        if not attachment_id:
                            continue

                        attachment_id = attachment_id.upper()
                        if attachment_id in attachment_id_picture_lookup:
                            attachments = attachment_id_picture_lookup[attachment_id]
                            attachments_list.append({"id": attachment_id, "attachments": attachments})

                if len(attachments_list) > 0:
                    dataset_attachment_lookup[ds_name] = {"name": ds_name, "path": dataset, "attachments": attachments_list}
                    arcpy.AddMessage("Found {} attachment id(s) for dataset {}".format(len(attachments_list), ds_name))

        scratch_folder = os.path.join(arcpy.env.scratchFolder, str(uuid.uuid4()))
        if os.path.exists(scratch_folder):
            arcpy.Delete_management(scratch_folder)

        arcpy.AddMessage("Creating scratch folder for temporary work: {}".format(scratch_folder))
        os.makedirs(scratch_folder)

        for dataset_attachment_info in dataset_attachment_lookup.values():
            ds_name = dataset_attachment_info["name"]
            arcpy.AddMessage("Creating attachments for dataset {}".format(ds_name))
            attachment_csv = os.path.join(scratch_folder, "{}.csv".format(ds_name))
            with open(attachment_csv, "w") as csv_file:
                writer = csv.writer(csv_file, delimiter=",")

                writer.writerow([attachment_id_field_name, attachment_path_field_name])
                for attachment_info in dataset_attachment_info["attachments"]:
                    attachment_id = attachment_info["id"]
                    for attachment in attachment_info["attachments"]:
                        writer.writerow([attachment_id, attachment])

            # the input feature class must first be GDB attachments enabled
            dataset = dataset_attachment_info["path"]
            arcpy.EnableAttachments_management(dataset)

            # use the match table with the Add Attachments tool
            arcpy.AddAttachments_management(dataset, attachment_id_field_name, attachment_csv, attachment_id_field_name,
                                            attachment_path_field_name, attachments_folder)

        try:
            arcpy.Delete_management(scratch_folder)
        except:
            arcpy.AddWarning("Unable to delete the scratch folder: {}".format(scratch_folder))


class UpdateAttachmentsZipTool(BaseTool):
    def __init__(self):
        super(UpdateAttachmentsZipTool, self).__init__(self.__class__.__name__)
        self.label = "Update Attachments in a Zipped Folder"
        self.description = "Traverses zipped folders to update attachments for data in a workspace"

        self.enfire_output_workspace_name = "enfire.gdb"
        self.enfire_zip_name = "enfire.zip"

    def getParameterInfo(self):
        """Define parameter definitions"""
        param_zip_input = arcpy.Parameter(displayName="Zipped Folder", name="zip_input", datatype="DEFile",
                                          parameterType="Required", direction="Input")
        param_map_name = arcpy.Parameter(displayName="Map Name", name="map_name",
                                                    datatype="GPString", parameterType="Required", direction="Input")
        param_attachment_id_field = arcpy.Parameter(displayName="Attachment ID Field", name="attachment_id_field",
                                                    datatype="GPString", parameterType="Required", direction="Input")
        param_template_mxd = arcpy.Parameter(displayName="Template Map Document", name="template_mxd",
                                             datatype="DEMapDocument", parameterType="Required", direction="Input")

        param_zip_output = arcpy.Parameter(displayName="Zipped Results", name="zip_output", datatype="DEFile",
                                           parameterType="Derived", direction="Output")

        params = [param_zip_input, param_map_name, param_attachment_id_field, param_template_mxd, param_zip_output]
        return params

    def tool_execute(self, parameters, messages):
        zip_input_file_path = parameters[0].valueAsText
        map_name = parameters[1].valueAsText
        attachment_id_field_name = parameters[2].valueAsText
        template_mxd_path = parameters[3].valueAsText

        scratch_root_folder = self.create_scratch_folder()
        scratch_output_folder = self.create_folder(os.path.join(scratch_root_folder, "output"))

        scratch_working_folder = os.path.join(scratch_root_folder, "working")
        output_folder, _ = self.build_updated_template_output(zip_input_file_path, map_name, attachment_id_field_name,
                                                              template_mxd_path, scratch_output_folder,
                                                              scratch_working_folder)

        scratch_zip_folder = self.create_scratch_folder()
        output_zip_file = self.zip_folder(output_folder, scratch_zip_folder, self.enfire_zip_name)

        try:
            arcpy.Delete_management(scratch_root_folder)
        except arcpy.ExecuteError:
            arcpy.AddWarning("Unable to delete scratch workspace")

        parameters[4].value = output_zip_file

    def build_updated_template_output(self, zip_input_file_path, map_name, attachment_id_field_name, template_mxd_path,
                                      output_folder, working_folder):
        original_workspace, attachments_folder = self.unzip_input(zip_input_file_path, working_folder)
        if not original_workspace or not attachments_folder:
            raise RuntimeError("Unable to find workspace and/or attachments folder in zip file")

        arcpy.AddMessage("Copying original workspace to output folder for updates")
        output_workspace = os.path.join(output_folder, self.enfire_output_workspace_name)
        arcpy.Copy_management(original_workspace, output_workspace)

        update_attachments_tool = UpdateAttachmentsTool()
        update_attachments_tool.update_attachments(output_workspace, attachment_id_field_name, attachments_folder)

        output_mxd_path = self.setup_template(template_mxd_path, map_name, output_folder)

        try:
            arcpy.Delete_management(working_folder)
        except arcpy.ExecuteError:
            arcpy.AddWarning("Unable to delete scratch workspace")

        return output_folder, output_mxd_path

    def unzip_input(self, zip_input_file_path, scratch_root_folder):
        scratch_folder = self.create_folder(os.path.join(scratch_root_folder, "unzip"))

        arcpy.AddMessage("Unzipping the input zip file: {}".format(os.path.basename(zip_input_file_path)))
        with ZipFile(zip_input_file_path, 'r') as zip_file:
            zip_file.extractall(scratch_folder)

        for path, subdirs, files in os.walk(scratch_folder, topdown=True):
            for subdir in subdirs:
                if subdir.lower().endswith(".gdb"):
                    workspace_path = os.path.join(path, subdir)
                    if not arcpy.Exists(workspace_path):
                        continue

                    if arcpy.Describe(workspace_path).dataType != "Workspace":
                        continue

                    workspace_dir = os.path.abspath(os.path.join(workspace_path, os.pardir))
                    arcpy.AddMessage("Found workspace {} and attachment folder {}".format(os.path.basename(workspace_path), os.path.basename(workspace_dir)))
                    return workspace_path, workspace_dir

        return None, None, None

    @staticmethod
    def get_all_file_paths(folder_path):
        arcpy.AddMessage("Getting all files for folder {}".format(os.path.basename(folder_path)))
        file_paths = []

        for root, directories, files in os.walk(folder_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                file_paths.append(file_path)

        return file_paths

    def zip_folder(self, zip_folder, output_folder, zip_name):
        files_to_zip = self.get_all_file_paths(zip_folder)

        arcpy.AddMessage("Zipping files")
        output_zip_file = os.path.join(output_folder, zip_name)
        with ZipFile(output_zip_file, 'w') as zip_file:
            for file_path in files_to_zip:
                zip_file_path = self.remove_prefix(file_path, zip_folder)
                zip_file.write(file_path, zip_file_path)

        return output_zip_file

    @staticmethod
    def remove_prefix(text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text

    @staticmethod
    def normalize_name(name):
        normalized_name = name.replace(" ", "_")
        return re.sub('[^A-Za-z0-9_]+', '', normalized_name)

    def setup_template(self, template_mxd_path, map_name, output_folder):
        arcpy.AddMessage("Copying template to output folder")
        normalized_map_name = self.normalize_name(map_name)
        output_template_mxd = os.path.join(output_folder, "{}.mxd".format(normalized_map_name))
        shutil.copy(template_mxd_path, output_template_mxd)

        mxd = arcpy.mapping.MapDocument(output_template_mxd)

        arcpy.AddMessage("Removing empty feature and group layers from the map document")
        zoom_extent = None
        for data_frame in arcpy.mapping.ListDataFrames(mxd):
            group_layers = []
            for layer in arcpy.mapping.ListLayers(mxd, "", data_frame):
                if layer.isGroupLayer:
                    group_layers.append({"layer": layer, "name": layer.longName, "count": 0})
                    continue

                if not layer.isFeatureLayer:
                    continue

                layer_count = int(arcpy.GetCount_management(layer).getOutput(0))
                if layer_count > 0:
                    layer_polygon = layer.getExtent(False).polygon
                    if not zoom_extent:
                        zoom_extent = layer_polygon
                    else:
                        zoom_extent = zoom_extent.union(layer_polygon)
                    continue

                arcpy.AddMessage("Removing layer {} since it does not have any data".format(layer.name))
                arcpy.mapping.RemoveLayer(data_frame, layer)

            for layer in arcpy.mapping.ListLayers(mxd, "", data_frame):
                self.update_group_layer_count(group_layers, layer)

            for group_layer in group_layers:
                if group_layer["count"] > 0:
                    continue

                group_layer_layer = group_layer["layer"]
                arcpy.AddMessage("Removing group layer {} since it does not have any underlying layers".format(group_layer_layer.longName))
                arcpy.mapping.RemoveLayer(data_frame, group_layer_layer)

        if zoom_extent:
            arcpy.AddMessage("Zooming map to data extent")
            data_frame = arcpy.mapping.ListDataFrames(mxd)[0]
            data_frame.extent = zoom_extent.extent

        mxd.save()
        del mxd

        return output_template_mxd

    @staticmethod
    def update_group_layer_count(group_layer_list, layer):
        for group_layer in group_layer_list:
            group_layer_name = group_layer["name"].lower()
            layer_name = layer.longName.lower()

            if group_layer_name == layer_name:
                return

            if layer_name.startswith(group_layer_name):
                group_layer["count"] = group_layer["count"] + 1

    def create_scratch_folder(self):
        scratch_folder = os.path.join(arcpy.env.scratchFolder, str(uuid.uuid4()))
        return self.create_folder(scratch_folder)

    @staticmethod
    def create_folder(folder):
        if os.path.exists(folder):
            arcpy.Delete_management(folder)

        arcpy.AddMessage("Creating folder for temporary work: {}".format(folder))
        os.makedirs(folder)

        return folder


class UpdateAttachmentsServerUploadTool(UpdateAttachmentsZipTool):
    def __init__(self):
        super(UpdateAttachmentsServerUploadTool, self).__init__()
        self.label = "Update Attachments in a Zipped Folder and Uploads Service"
        self.description = "Traverses zipped folders to update attachments for data in a workspace and uploads map"

    def getParameterInfo(self):
        """Define parameter definitions"""
        param_zip_input = arcpy.Parameter(displayName="Zipped Folder", name="zip_input", datatype="DEFile",
                                          parameterType="Required", direction="Input")
        param_map_name = arcpy.Parameter(displayName="Map Name", name="map_name",
                                         datatype="GPString", parameterType="Required", direction="Input")
        param_summary = arcpy.Parameter(displayName="Service Summary", name="service_summary",
                                        datatype="GPString", parameterType="Required", direction="Input")
        param_tags = arcpy.Parameter(displayName="Service Tags", name="service_tags",
                                     datatype="GPString", parameterType="Required", direction="Input")
        param_attachment_id_field = arcpy.Parameter(displayName="Attachment ID Field", name="attachment_id_field",
                                                    datatype="GPString", parameterType="Required", direction="Input")
        param_template_mxd = arcpy.Parameter(displayName="Template Map Document", name="template_mxd",
                                             datatype="DEMapDocument", parameterType="Required", direction="Input")
        param_ags_connection = arcpy.Parameter(displayName="Template Map Document", name="template_mxd",
                                               datatype="DEServerConnection", parameterType="Required",
                                               direction="Input")

        params = [param_zip_input, param_map_name, param_summary, param_tags, param_attachment_id_field,
                  param_template_mxd, param_ags_connection]
        return params

    def tool_execute(self, parameters, messages):
        zip_input_file_path = parameters[0].valueAsText
        map_name = parameters[1].valueAsText
        summary = parameters[2].valueAsText
        tags = parameters[3].valueAsText
        attachment_id_field_name = parameters[4].valueAsText
        template_mxd_path = parameters[5].valueAsText
        ags_connection = parameters[6].valueAsText

        scratch_root_folder = self.create_scratch_folder()
        scratch_output_folder = self.create_folder(os.path.join(scratch_root_folder, "output"))

        scratch_working_folder = os.path.join(scratch_root_folder, "working")
        output_folder, mxd_path = self.build_updated_template_output(zip_input_file_path, map_name,
                                                                              attachment_id_field_name,
                                                                              template_mxd_path, scratch_output_folder,
                                                                              scratch_working_folder)

        # try:
        #     arcpy.Delete_management(scratch_root_folder)
        # except arcpy.ExecuteError:
        #     arcpy.AddWarning("Unable to delete scratch workspace")

    def upload_map_service(self, mxd_path, service_name, summary, tags, ags_connection, working_folder):
        mxd = arcpy.mapping.MapDocument(mxd_path)
        working_folder = self.create_folder(working_folder)

        arcpy.AddMessage("Creating Map Service Draft")
        arcpy.mapping.CreateMapSDDraft(mxd, sd_draft, service_name, 'ARCGIS_SERVER', ags_connection, True, None, summary
                                       , tags)


if __name__ == "__main__":
    tool = UpdateAttachmentsZipTool()
    parameters = tool.getParameterInfo()

    parameters[0].value = r"C:\Users\RDAGCAFP\Documents\ArcGIS\Projects\EnfireAGETools\ENFIRE_8_Camp_Grayling_MXD_GeoDB_attachments_6_27_2019.zip"
    parameters[1].value = "Enfire Test Name 123!!"
    parameters[2].value = "GUID_PK"
    parameters[3].value = r"C:\Users\RDAGCAFP\Documents\ArcGIS\Projects\EnfireAGETools\template\ENFIRE_Template.mxd"

    tool.execute(parameters, arcpy)
