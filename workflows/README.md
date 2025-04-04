#### 文件概述
    该文件提供了将 XML 格式的 workflow 定义转换为 JSON 格式的 Manifest 文件的功能。
    通过该模块，可以将 Pegasus 工作流定义文件（XML 格式）转换为适用于 backfiling 的 JSON Manifest 文件。

#### 主要内容
1. **模块说明**
   - 该模块提供了一个函数 `convert_xml_wf_to_json_manifest`，用于将 XML 格式的 workflow 定义转换为 JSON 格式的 Manifest 文件。
   - 提供了辅助函数来处理和转换工作流数据，包括获取和操作任务列表、依赖关系、以及生成资源步骤等。

2. **导入的库**
   - `json`: 用于处理 JSON 数据。
   - `pygraphviz as pgv`: 用于生成和操作图结构。
   - `xml.etree.ElementTree as ET`: 用于解析 XML 文件。
   - `sys`: 用于处理系统相关的功能，如进度输出。

3. **主要函数**
   - `convert_xml_wf_to_json_manifest(xml_route, json_route, grouped_jobs=[], max_cores=None, namespace=None)`: 
        将 XML 格式的 workflow 定义转换为 JSON 格式的 Manifest 文件。
     - 参数:
       - `xml_route`: XML 文件的路径。
       - `json_route`: 输出 JSON 文件的路径。
       - `grouped_jobs`: 需要分组的任务名称列表。
       - `max_cores`: 每个任务的最大核心数。
       - `namespace`: XML 文件的命名空间。
     - 功能:
       - 读取 XML 文件。
       - 获取任务和依赖关系。
       - 合并任务。
       - 合并依赖关系。
       - 处理任务序列。
       - 重命名任务。
       - 生成 Manifest 字典。
       - 将 Manifest 字典写入 JSON 文件。

4. **辅助函数**
   - `_encode_manifest_dic(jobs, deps)`: 生成 Manifest 字典。
   - `_produce_dot_graph(jobs, deps)`: 生成 DOT 格式的图结构。
   - `_produce_tasks(jobs)`: 生成任务列表。
   - `get_inverse_deps(deps)`: 获取反向依赖关系。
   - `_produce_resource_steps(jobs, deps)`: 生成资源步骤。
   - `_job_can_run(job)`: 检查任务是否可以运行。
   - `_rename_jobs(jobs, deps, new_job_name)`: 重命名任务。
   - `_get_jobs_names(jobs, deps)`: 获取任务的新名称。
   - `get_tag(tag, namespace=None)`: 获取带有命名空间的标签。
   - `_get_namespace(xml_wf)`: 获取 XML 文件的命名空间。
   - `get_runtime(cad)`: 获取运行时间。
   - `_get_jobs_and_deps(xml_wf, namespace=None)`: 获取任务和依赖关系。
   - `_reshape_job(job, max_cores)`: 重塑任务。
   - `_fuse_sequence_jobs(jobs, deps)`: 合并序列任务。
   - `_fuse_two_jobs_sequence(jobs, deps, job_orig, job_dst)`: 合并两个序列任务。
   - `_get_job(job_list, job_id)`: 获取任务。
   - `extract_groups_cores(grouped_jobs, max_cores=None)`: 提取分组任务的核心数。
   - `_fuse_jobs(job_list, grouped_jobs, max_cores=None)`: 合并任务。
   - `_fuse_deps(dep_dic, job_fusing_dic)`: 合并依赖关系。

### 说明文档大纲

#### 1. 模块概述
- **目的**: 提供将 XML 格式的 workflow 定义转换为 JSON 格式的 Manifest 文件的功能。
- **使用场景**: 在需要将 Pegasus 工作流定义文件转换为适用于 backfiling 的 JSON Manifest 文件时使用。

#### 2. 主要功能
- **XML 到 JSON 转换**: 将 XML 格式的 workflow 定义转换为 JSON 格式的 Manifest 文件。
- **任务和依赖关系处理**: 提供辅助函数来处理和转换工作流数据，包括获取和操作任务列表、依赖关系、以及生成资源步骤等。

#### 3. 使用指南
##### 3.1 转换 XML 到 JSON
- **函数**: `convert_xml_wf_to_json_manifest(xml_route, json_route, grouped_jobs=[], max_cores=None, namespace=None)`
- **参数**:
  - `xml_route`: XML 文件的路径。
  - `json_route`: 输出 JSON 文件的路径。
  - `grouped_jobs`: 需要分组的任务名称列表。
  - `max_cores`: 每个任务的最大核心数。
  - `namespace`: XML 文件的命名空间。
- **示例**:
  ```python
  from workflows import convert_xml_wf_to_json_manifest

  # 转换 XML 到 JSON
  convert_xml_wf_to_json_manifest(
      xml_route="path/to/workflow.xml",
      json_route="path/to/manifest.json",
      grouped_jobs=["job1", "job2:4"],
      max_cores=8,
      namespace="http://pegasus.isi.edu/schema/DAX"
  )
  ```


#### 4. 辅助函数

##### 4.1 `_encode_manifest_dic(jobs, deps)`
- **功能**: 生成 Manifest 字典。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。

##### 4.2 `_produce_dot_graph(jobs, deps)`
- **功能**: 生成 DOT 格式的图结构。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。

##### 4.3 `_produce_tasks(jobs)`
- **功能**: 生成任务列表。
- **参数**:
  - `jobs`: 任务列表。

##### 4.4 `get_inverse_deps(deps)`
- **功能**: 获取反向依赖关系。
- **参数**:
  - `deps`: 依赖关系字典。

##### 4.5 `_produce_resource_steps(jobs, deps)`
- **功能**: 生成资源步骤。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。

##### 4.6 `_job_can_run(job)`
- **功能**: 检查任务是否可以运行。
- **参数**:
  - `job`: 任务字典。

##### 4.7 `_rename_jobs(jobs, deps, new_job_name)`
- **功能**: 重命名任务。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。
  - `new_job_name`: 新任务名称字典。

##### 4.8 `_get_jobs_names(jobs, deps)`
- **功能**: 获取任务的新名称。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。

##### 4.9 `get_tag(tag, namespace=None)`
- **功能**: 获取带有命名空间的标签。
- **参数**:
  - `tag`: 标签名称。
  - `namespace`: 命名空间。

##### 4.10 `_get_namespace(xml_wf)`
- **功能**: 获取 XML 文件的命名空间。
- **参数**:
  - `xml_wf`: XML 文件对象。

##### 4.11 `get_runtime(cad)`
- **功能**: 获取运行时间。
- **参数**:
  - `cad`: 运行时间字符串。

##### 4.12 `_get_jobs_and_deps(xml_wf, namespace=None)`
- **功能**: 获取任务和依赖关系。
- **参数**:
  - `xml_wf`: XML 文件对象。
  - `namespace`: 命名空间。

##### 4.13 `_reshape_job(job, max_cores)`
- **功能**: 重塑任务。
- **参数**:
  - `job`: 任务字典。
  - `max_cores`: 最大核心数。

##### 4.14 `_fuse_sequence_jobs(jobs, deps)`
- **功能**: 合并序列任务。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。

##### 4.15 `_fuse_two_jobs_sequence(jobs, deps, job_orig, job_dst)`
- **功能**: 合并两个序列任务。
- **参数**:
  - `jobs`: 任务列表。
  - `deps`: 依赖关系字典。
  - `job_orig`: 原始任务 ID。
  - `job_dst`: 目标任务 ID。

##### 4.16 `_get_job(job_list, job_id)`
- **功能**: 获取任务。
- **参数**:
  - `job_list`: 任务列表。
  - `job_id`: 任务 ID。

##### 4.17 `extract_groups_cores(grouped_jobs, max_cores=None)`
- **功能**: 提取分组任务的核心数。
- **参数**:
  - `grouped_jobs`: 分组任务列表。
  - `max_cores`: 最大核心数。

##### 4.18 `_fuse_jobs(job_list, grouped_jobs, max_cores=None)`
- **功能**: 合并任务。
- **参数**:
  - `job_list`: 任务列表。
  - `grouped_jobs`: 分组任务列表。
  - `max_cores`: 最大核心数。

##### 4.19 `_fuse_deps(dep_dic, job_fusing_dic)`
- **功能**: 合并依赖关系。
- **参数**:
  - `dep_dic`: 依赖关系字典。
  - `job_fusing_dic`: 任务合并字典。

#### 5. 示例代码
- **转换 XML 到 JSON**:
  ```python
  from workflows import convert_xml_wf_to_json_manifest

  # 转换 XML 到 JSON
  convert_xml_wf_to_json_manifest(
      xml_route="path/to/workflow.xml",
      json_route="path/to/manifest.json",
      grouped_jobs=["job1", "job2:4"],
      max_cores=8,
      namespace="http://pegasus.isi.edu/schema/DAX"
  )
  ```


#### 6. 注意事项
- 确保 XML 文件的格式符合 Pegasus 工作流定义格式。
- 如果设置了 `grouped_jobs` 参数，确保任务名称正确。
- 如果设置了 `max_cores` 参数，确保任务的核心数不超过最大核心数。

