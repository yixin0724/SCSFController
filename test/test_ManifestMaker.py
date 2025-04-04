""" Unittests for the code convering the pegasus XML workflow models
into the wf_aware manifests. 

 python -m unittest test_ManiestMaker
 需要自行测试
"""

import unittest
from workflows import (_get_jobs_and_deps, _fuse_jobs,
                       _fuse_deps, _get_jobs_names, _rename_jobs,
                       _produce_resource_steps, _encode_manifest_dic,
                       _reshape_job, _fuse_sequence_jobs, 
                       _fuse_two_jobs_sequence)
import xml.etree.ElementTree as ET

class TestManifestMaker(unittest.TestCase):
     
    def test_get_jobs_and_deps(self):
        """
        测试从XML工作流文件中提取作业和依赖关系的功能。
        该测试用例验证了_get_jobs_and_deps函数是否能正确解析XML文件，
        并返回预期的作业配置和作业间的依赖关系。
        """
        # 解析XML工作流文件
        xml_wf=ET.parse("./floodplain.xml")
        # 调用_get_jobs_and_deps函数，获取作业列表和依赖关系
        jobs, deps = _get_jobs_and_deps(xml_wf)
        # 验证作业列表是否与预期一致
        self.assertEqual(jobs,
                         [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ])
        # 验证依赖关系是否与预期一致
        self.assertEqual(deps,
                         {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
                         )
    def test_fuse_jobs(self):
        """
        测试融合作业函数的行为。
        此测试函数旨在验证当提供一组作业和一个需要融合的作业名称列表时，
        _fuse_jobs 函数是否按预期工作。具体来说，它检查函数是否正确地将具有相同名称的作业融合，
        并更新作业列表和映射字典。
        """
        # 定义一组模拟的作业数据，每个作业包含id、name、runtime和cores字段。
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                {"id":"sin2", "name":"SWAN Inner North",
                           "runtime":12400,"cores":150},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ]
        # 调用_fuse_jobs函数，传入作业列表和需要融合的作业名称列表。
        # 融合名称为"SWAN Inner North"的作业。
        new_jobs, fused_jobs_dic = _fuse_jobs(jobs, ["SWAN Inner North"])
        # 断言新的作业列表是否与预期的结果匹配。
        # 融合后的"SWAN Inner North"作业应该具有合并的核心数。
        self.assertEqual(new_jobs,
               [{"id":"SWAN Inner North", "name":"SWAN Inner North",
                           "runtime":14400,"cores":310},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ])
        # 断言融合作业的字典是否正确，记录了融合作业的id。
        # 这里应该包含原始"SWAN Inner North"作业的id。
        self.assertEqual(fused_jobs_dic, {"SWAN Inner North":["sin", "sin2"]})
    
    def test_reshape_job(self):
        """
        测试调整任务形状的功能。

        此测试验证了当任务的运行时间与核心数需要根据特定逻辑进行调整时，
        是否能够得到预期的结果。这是为了确保任务可以在不同的环境下有效运行，
        而不会超出资源限制或造成资源浪费。
        """
        # 创建一个示例任务字典，包含任务的各种属性，如ID、名称、运行时间、核心数等。
        job = {"id":"sin", "name":"SWAN Inner North",
                           "runtime":20,"cores":100,
                           "task_count":20,
                           "acc_runtime":400}
        # 调用待测试的函数，传入任务和新的核心数限制。
        new_runtime, max_cores = _reshape_job(job, 10)
        # 验证调整后的运行时间是否符合预期。
        self.assertEqual(new_runtime,200)
        # 验证调整后的最大核心数是否符合预期。
        self.assertEqual(max_cores,10)
    
    def test_fuse_jobs_max_cores(self):
        """
        测试当指定最大内核数时，_fuse_jobs函数是否能正确融合作业。
        此测试用例的目的是验证在给定作业列表和最大内核数限制的情况下，
        _fuse_jobs函数是否能正确融合具有相同名称的作业，并且不超过最大内核数限制。
        """
        # 定义一个作业列表，每个作业包含id、name、runtime（运行时间）和cores（内核数）
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                {"id":"sin2", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ]
        # 调用_fuse_jobs函数，传入作业列表、需要融合的作业名称列表和最大内核数限制
        new_jobs, fused_jobs_dic = _fuse_jobs(jobs, ["SWAN Inner North"],
                                              max_cores=160)

        # 断言融合后的作业列表是否正确
        self.assertEqual(new_jobs,
               [{"id":"SWAN Inner North", "name":"SWAN Inner North",
                           "runtime":28800,"cores":160},
                {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                   "cores":256},
                 {"id":"sos", "name":"SWAN Outer South", 
                  "runtime":28800, "cores":10},
                 {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                  "cores":256},
                 {"id":"son", "name":"SWAN Outer North",
                  "runtime":46800, "cores":8},
                 {"id":"sis", "name":"SWAN Inner South",
                  "runtime":10800, "cores":192},
                 {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                  "cores":256}
                  ])
        # 断言融合作业的字典是否正确，该字典应包含原作业id以便跟踪
        self.assertEqual(fused_jobs_dic, {"SWAN Inner North":["sin", "sin2"]})
    
        
        
        
    def test_fuse_deps(self):
        """
        测试依赖关系融合功能。
        此方法旨在验证依赖关系融合的正确性。它通过将一系列任务的依赖关系
        融合成更简洁的映射，来检查_fuse_deps函数是否按预期工作。
        """
        # 原始依赖关系字典，定义了各个任务及其依赖的前置任务。
        orig_deps = {"son":["sin", "sin2"],
                    "adcirc":["sin", "sin2", "sos", "son", "sis"],
                    "sis":["adcirc2"],
                    "sin":["adcirc2"],
                    "sin2":["adcirc2"],
                    "ww3":["sos", "son"],
                    "sos":["sis"]}
        # 融合任务字典，将一系列任务融合为一个或几个任务组。
        fused_jobs_dic = {"SWAN Inner North":["sin", "sin2"]}
        # 调用_fuse_deps函数来融合依赖关系，并将结果赋值给fused_deps。
        fused_deps=_fuse_deps(orig_deps, fused_jobs_dic)
        # 遍历融合后的依赖关系，对每个任务的依赖列表进行排序。
        for (key, deps) in fused_deps.iteritems():
            fused_deps[key]=sorted(deps)
        # 断言融合并排序后的依赖关系与预期的结果是否一致。
        self.assertEqual((fused_deps),
                        ({"son":["SWAN Inner North"],
                        "adcirc":sorted(["SWAN Inner North","sos", "son",
                                         "sis"]),
                        "sis":["adcirc2"],
                        "SWAN Inner North":["adcirc2"],
                        "ww3":sorted(["sos", "son"]),
                        "sos":["sis"]}))
    
    def test_fuse_two_jobs_sequence(self):
        """
        测试合并两个作业序列的功能。
        此测试函数旨在验证当两个作业被合并时，它们的属性（如运行时间和核心数）是否正确合并，
        以及依赖关系是否得到正确更新。
        """
        # 初始化作业列表，每个作业包含其ID、名称、运行时间和核心数
        jobs = [{"id":"job1", "name":"acction1",
                        "runtime":100,"cores":10},
                {"id":"job2", "name":"acction2",
                        "runtime":125,"cores":10},
                {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                {"id":"job4", "name":"acction4",
                        "runtime":100,"cores":10},
                {"id":"job5", "name":"acction5",
                        "runtime":125,"cores":10}]
        # 初始化作业依赖关系字典，定义作业之间的执行顺序
        deps = {"job1":["job2"],
                "job2":["job3"],
                "job3":["job4"],
                "job4":["job5"]}
        # 第一次合并作业序列，合并job1和job2
        _fuse_two_jobs_sequence(jobs, deps, "job1", "job2")
        # 验证合并后作业列表是否正确更新
        self.assertEqual(jobs,
                         [{"id":"job1", "name":"acction1",
                        "runtime":225,"cores":10},
                          {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                          {"id":"job4", "name":"acction4",
                        "runtime":100,"cores":10},
                          {"id":"job5", "name":"acction5",
                        "runtime":125,"cores":10}]
                         )
        # 验证依赖关系是否正确更新
        self.assertEqual(deps,
                         {"job1":["job3"],
                          "job3":["job4"],
                          "job4":["job5"]})

        # 第二次合并作业序列，合并job4和job5
        _fuse_two_jobs_sequence(jobs, deps, "job4", "job5")
        # 再次验证合并后作业列表是否正确更新
        self.assertEqual(jobs,
                         [{"id":"job1", "name":"acction1",
                        "runtime":225,"cores":10},
                          {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                          {"id":"job4", "name":"acction4",
                        "runtime":225,"cores":10}]
                         )
        # 再次验证依赖关系是否正确更新
        self.assertEqual(deps,
                         {"job1":["job3"],
                          "job3":["job4"]})
    
    def test_fuse_sequence_jobs(self):
        """
        测试融合连续作业的功能。
        此测试用例旨在验证当作业之间存在连续依赖关系时，系统能否正确地融合这些作业，以减少调度开销或提高资源利用率。
        通过预定义的作业列表和依赖关系，测试_fuse_sequence_jobs函数是否能正确地融合作业并更新依赖关系。
        """
        jobs = [{"id":"job1", "name":"acction1",
                        "runtime":100,"cores":10},
                {"id":"job2", "name":"acction2",
                        "runtime":125,"cores":10},
                {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                {"id":"job4", "name":"acction4",
                        "runtime":100,"cores":10},
                {"id":"job5", "name":"acction5",
                        "runtime":125,"cores":10}]
        deps = {"job1":["job2"],
                "job2":["job3"],
                "job3":["job4"],
                "job4":["job5"]}
        # 调用待测试的函数，传入作业列表和依赖关系。
        _fuse_sequence_jobs(jobs, deps)
        self.assertEqual(jobs,
                         [{"id":"job1", "name":"acction1",
                        "runtime":225,"cores":10},
                          {"id":"job3", "name":"acction3",
                        "runtime":125,"cores":15},
                          {"id":"job4", "name":"acction4",
                        "runtime":225,"cores":10}]
                         )
        self.assertEqual(deps,
                         {"job1":["job3"],
                          "job3":["job4"]})
        
    
    
    def test_get_job_names(self):
        """
        测试_get_job_names函数，该函数的主要作用是根据作业信息和依赖关系，
        为每个作业分配一个特定的名称标识。这个测试函数验证了函数是否能正确地
        生成预期的作业名称映射。
        """
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        # 定义作业之间的依赖关系，某些作业需要在其他作业完成后才能开始
        deps = {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
        # 调用_get_job_names函数，传入作业信息和依赖关系，获取作业名称映射
        job_names = _get_jobs_names(jobs, deps)
        # 使用断言来验证函数的输出是否与预期的作业名称映射完全一致
        self.assertEqual(job_names,
                         {"sin": "S4",
                          "adcirc2": "S6",
                          "sos": "S3",
                          "adcirc": "S0",
                          "son": "S5",
                          "sis": "S2",
                          "ww3": "S1"
                          })     
    def test_rename_jobs(self):
        """
        测试重命名作业的功能。
        此测试用例旨在验证将作业名称映射到新名称的功能，并确保作业列表和作业间依赖关系正确更新。
        """
        # 定义原始作业列表，每个作业包含id、name、runtime和cores属性
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        # 定义作业间的依赖关系，键为作业id，值为依赖的作业id列表
        deps = {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
        # 定义作业的新名称映射，键为原作业id，值为新的作业id
        new_job_names={"sin": "S2",
                          "adcirc2": "S6",
                          "sos": "S3",
                          "adcirc": "S0",
                          "son": "S4",
                          "sis": "S5",
                          "ww3": "S1"
                          }
        # 调用_rename_jobs函数，传入作业列表、依赖关系和新名称映射，返回更新后的作业列表和依赖关系
        new_jobs, new_deps = _rename_jobs(jobs, deps, new_job_names)
        # 断言更新后的作业列表是否符合预期，包括id、name、runtime和cores属性
        self.assertEqual(jobs,
                         [{"id":"S2", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"S6", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"S3", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"S0", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"S4", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"S5", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"S1", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ])
        # 断言更新后的作业依赖关系是否符合预期，包括新的作业id和依赖的作业id列表
        self.assertEqual(new_deps,
                         {"S4":["S2"],
                          "S0":["S2", "S3", "S4", "S5"],
                          "S5":["S6"],
                          "S2":["S6"],
                          "S1":["S3", "S4"],
                          "S3":["S5"]
                          }
                         )
    def test_produce_resource_steps(self):
        """
        测试生产资源步骤的函数。
        该函数用于验证_produce_resource_steps函数是否能够正确地根据作业信息和依赖关系生成资源步骤。
        """
        jobs = [{"id":"sin", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"adcirc2", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"sos", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"adcirc", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"son", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"sis", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"ww3", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        deps = {"son":["sin"],
                          "adcirc":["sin", "sos", "son", "sis"],
                          "sis":["adcirc2"],
                          "sin":["adcirc2"],
                          "ww3":["sos", "son"],
                          "sos":["sis"]
                          }
        # 调用_produce_resource_steps函数，传入作业信息和依赖关系，生成资源步骤
        resource_steps = _produce_resource_steps(jobs, deps)
        # 断言生成的资源步骤与预期的资源步骤列表相等
        self.assertEqual(resource_steps,
                         [{"num_cores": 512,
                           "end_time":   3600},
                          {"num_cores": 256,
                           "end_time":   39600},
                          {"num_cores": 18,
                           "end_time":   39600+28800},
                          {"num_cores": 200,
                           "end_time":   39600+28800+10800},
                          {"num_cores": 8,
                           "end_time":   39600+46800},
                          {"num_cores": 160,
                           "end_time":   39600+46800+14400},
                          {"num_cores": 256,
                           "end_time":   39600+46800+14400+16200}
                          ]
                         )
        
    def test_enconde_manifest_dic(self):
        """
        测试_encode_manifest_dic函数的正确性。

        该函数将工作列表和依赖关系字典转换为一个特定格式的字典。
        主要目的是验证转换后的字典是否与预期的结构和数据完全匹配。
        """
        jobs = [{"id":"S2", "name":"SWAN Inner North",
                           "runtime":14400,"cores":160},
                          {"id":"S6", "name":"Adcirc", "runtime":16200.0, 
                           "cores":256},
                         {"id":"S3", "name":"SWAN Outer South", 
                          "runtime":28800, "cores":10},
                         {"id":"S0", "name":"Adcirc", "runtime":39600,
                          "cores":256},
                         {"id":"S4", "name":"SWAN Outer North",
                          "runtime":46800, "cores":8},
                         {"id":"S5", "name":"SWAN Inner South",
                          "runtime":10800, "cores":192},
                         {"id":"S1", "name":"WaveWatchIII", "runtime":3600,
                          "cores":256}
                          ]
        deps = {"S4":["S2"],
                  "S0":["S2", "S3", "S4", "S5"],
                  "S5":["S6"],
                  "S2":["S6"],
                  "S1":["S3", "S4"],
                  "S3":["S5"]
                  }
        # 调用_encode_manifest_dic函数来编码作业和依赖关系
        manifest_dic=_encode_manifest_dic(jobs, deps)
        # 对生成的字典中的任务列表进行排序，以确保比较时顺序一致
        manifest_dic["tasks"] = sorted(manifest_dic["tasks"])
        # 设置最大差异输出为None，以便在断言失败时看到完整的差异
        self.maxDiff=None
        # 断言编码后的字典与预期的结构和数据完全匹配
        self.assertEqual(manifest_dic,
                         {"tasks": sorted([{"id":"S2", "name":"S2",
                           "runtime_limit":14460,
                           "runtime_sim":14400,
                           "number_of_cores":160,
                           "execution_cmd":"./S2.py"},
                          {"id":"S6", "name":"S6", 
                           "runtime_limit":16260.0,
                           "runtime_sim":16200.0, 
                           "number_of_cores":256,
                           "execution_cmd":"./S6.py"},
                         {"id":"S3", "name":"S3", 
                          "runtime_limit":28860,
                          "runtime_sim":28800,
                          "number_of_cores":10,
                           "execution_cmd":"./S3.py"},
                         {"id":"S0", "name":"S0",
                          "runtime_limit":39660,
                          "runtime_sim":39600,
                          "number_of_cores":256,
                           "execution_cmd":"./S0.py"},
                         {"id":"S4", "name":"S4",
                          "runtime_limit":46860, 
                          "runtime_sim":46800, "number_of_cores":8,
                           "execution_cmd":"./S4.py"},
                         {"id":"S5", "name":"S5",
                          "runtime_limit":10860, 
                          "runtime_sim":10800, "number_of_cores":192,
                           "execution_cmd":"./S5.py"},
                         {"id":"S1", "name":"S1",
                          "runtime_limit":3660,
                          "runtime_sim":3600,
                          "number_of_cores":256,
                           "execution_cmd":"./S1.py"}]),
                         "resource_steps": [{"num_cores": 512,
                           "end_time":   3600},
                          {"num_cores": 256,
                           "end_time":   39600},
                          {"num_cores": 18,
                           "end_time":   39600+28800},
                          {"num_cores": 200,
                           "end_time":   39600+28800+10800},
                          {"num_cores": 8,
                           "end_time":   39600+46800},
                          {"num_cores": 160,
                           "end_time":   39600+46800+14400},
                          {"num_cores": 256,
                           "end_time":   float(39600+46800+14400+16200)}
                          ],
                          "max_cores":512,
                          "total_runtime": float(39600+46800+14400+16200),
                          "dot_dag":u'strict digraph "" {\n\tS2 -> S6;\n\tS3 -> S5;\n\tS0 -> S2;\n\tS0 -> S3;\n\tS0 -> S4;\n\tS0 -> S5;\n\tS4 -> S2;\n\tS5 -> S6;\n\tS1 -> S3;\n\tS1 -> S4;\n}\n'
                          })
        