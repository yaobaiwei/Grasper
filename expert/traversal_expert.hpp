/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef TRAVERSAL_EXPERT_HPP_
#define TRAVERSAL_EXPERT_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "expert/abstract_expert.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

// IN-OUT-BOTH

using namespace std::placeholders;

class TraversalExpert : public AbstractExpert {
 public:
    TraversalExpert(int id, DataStore* data_store, int num_thread, AbstractMailbox * mailbox, CoreAffinity * core_affinity) : AbstractExpert(id, data_store, core_affinity), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::TRAVERSAL) {
        config_ = Config::GetInstance();
    }

    // TraversalExpertObject->Params;
    //  inType--outType--dir--lid
    //  dir: Direcntion:
    //           Vertex: IN/OUT/BOTH
    //                   INE/OUTE/BOTHE
    //             Edge: INV/OUTV/BOTHV
    //  lid: label_id (e.g. g.V().out("created"))
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // Get params
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
        Element_T outType = (Element_T) Tool::value_t2int(expert_obj.params.at(1));
        Direction_T dir = (Direction_T) Tool::value_t2int(expert_obj.params.at(2));
        int lid = Tool::value_t2int(expert_obj.params.at(3));

        // Get Result
        if (inType == Element_T::VERTEX) {
            if (outType == Element_T::VERTEX) {
                GetNeighborOfVertex(tid, lid, dir, msg.data);
            } else if (outType == Element_T::EDGE) {
                GetEdgeOfVertex(tid, lid, dir, msg.data);
            } else {
                cout << "Wrong Out Element Type: " << outType << endl;
                return;
            }
        } else if (inType == Element_T::EDGE) {
            if (outType == Element_T::VERTEX) {
                GetVertexOfEdge(tid, lid, dir, msg.data);
            } else {
                cout << "Wrong Out Element Type: " << outType << endl;
                return;
            }
        } else {
            cout << "Wrong In Element Type: " << inType << endl;
            return;
        }

        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(expert_objs, msg.data, num_thread_, data_store_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

 private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Cache
    ExpertCache cache;
    Config* config_;

    // ============Vertex===============
    // Get IN/OUT/BOTH of Vertex
    void GetNeighborOfVertex(int tid, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto& pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                // Get the current vertex id and use it to get vertex instance
                vid_t cur_vtx_id(Tool::value_t2int(value));
                Vertex* vtx = data_store_->GetVertex(cur_vtx_id);

                // for each neighbor, create a new value_t and store into newData
                // IN & BOTH
                if (dir != Direction_T::OUT) {
                    for (auto & in_nb : vtx->in_nbs) {  // in_nb : vid_t
                        // Get edge_id
                        if (lid > 0) {
                            eid_t e_id(cur_vtx_id.value(), in_nb.value());
                            label_t label;
                            get_label_for_edge(tid, e_id, label);

                            if (label != lid) {
                                continue;
                            }
                        }
                        value_t new_value;
                        Tool::str2int(to_string(in_nb.value()), new_value);
                        newData.push_back(new_value);
                    }
                }
                // OUT & BOTH
                if (dir != Direction_T::IN) {
                    for (auto & out_nb : vtx->out_nbs) {
                        if (lid > 0) {
                            eid_t e_id(out_nb.value(), cur_vtx_id.value());
                            label_t label;
                            get_label_for_edge(tid, e_id, label);

                            if (label != lid) {
                                continue;
                            }
                        }
                        value_t new_value;
                        Tool::str2int(to_string(out_nb.value()), new_value);
                        newData.push_back(new_value);
                    }
                }
            }

            // Replace pair.second with new data
            pair.second.swap(newData);
        }
    }

    // Get IN/OUT/BOTH-E of Vertex
    void GetEdgeOfVertex(int tid, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto& pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                // Get the current vertex id and use it to get vertex instance
                vid_t cur_vtx_id(Tool::value_t2int(value));
                Vertex* vtx = data_store_->GetVertex(cur_vtx_id);

                if (dir != Direction_T::OUT) {
                    for (auto & in_nb : vtx->in_nbs) {  // in_nb : vid_t
                        // Get edge_id
                        eid_t e_id(cur_vtx_id.value(), in_nb.value());
                        if (lid > 0) {
                            label_t label;
                            get_label_for_edge(tid, e_id, label);

                            if (label != lid) {
                                continue;
                            }
                        }
                        value_t new_value;
                        Tool::str2uint64_t(to_string(e_id.value()), new_value);
                        newData.push_back(new_value);
                    }
                }

                if (dir != Direction_T::IN) {
                    for (auto & out_nb : vtx->out_nbs) {
                        // Get edge_id
                        eid_t e_id(out_nb.value(), cur_vtx_id.value());
                        if (lid > 0) {
                            label_t label;
                            get_label_for_edge(tid, e_id, label);

                            if (label != lid) {
                                continue;
                            }
                        }
                        value_t new_value;
                        Tool::str2uint64_t(to_string(e_id.value()), new_value);
                        newData.push_back(new_value);
                    }
                }
            }

            // Replace pair.second with new data
            pair.second.swap(newData);
        }
    }

    // =============Edge================
    void GetVertexOfEdge(int tid, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                uint64_t eid_value = Tool::value_t2uint64_t(value);
                uint64_t in_v = eid_value >> VID_BITS;
                uint64_t out_v = eid_value - (in_v << VID_BITS);

                if (dir == Direction_T::IN) {
                    value_t new_value;
                    Tool::str2int(to_string(in_v), new_value);
                    newData.push_back(new_value);
                } else if (dir == Direction_T::OUT) {
                    value_t new_value;
                    Tool::str2int(to_string(out_v), new_value);
                    newData.push_back(new_value);
                } else if (dir == Direction_T::BOTH) {
                    value_t new_value_in;
                    value_t new_value_out;
                    Tool::str2int(to_string(in_v), new_value_in);
                    Tool::str2int(to_string(out_v), new_value_out);
                    newData.push_back(new_value_in);
                    newData.push_back(new_value_out);
                } else {
                    cout << "Wrong Direction Type" << endl;
                    return;
                }
            }

            // Replace pair.second with new data
            pair.second.swap(newData);
        }
    }

    void get_label_for_edge(int tid, eid_t e_id, label_t & label) {
        if (data_store_->EPKeyIsLocal(epid_t(e_id, 0)) || !config_->global_enable_caching) {
            data_store_->GetLabelForEdge(tid, e_id, label);
        } else {
            if (!cache.get_label_from_cache(e_id.value(), label)) {
                data_store_->GetLabelForEdge(tid, e_id, label);
                cache.insert_label(e_id.value(), label);
            }
        }
    }
};
#endif /* TRAVERSAL_EXPERT_HPP_ */
