/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef BRPC_MODULE_H
#define BRPC_MODULE_H

namespace eloq {

class EloqModule {
public:
    virtual ~EloqModule() {};

    virtual void ExtThdStart(int thd_id) = 0;
    virtual void ExtThdEnd(int thd_id) = 0;
    virtual void Process(int thd_id) = 0;
    virtual bool HasTask(int thd_id) const = 0;

    static bool NotifyWorker(int thd_id);
};

}  // namespace bthread

#endif //BRPC_MODULE_H
