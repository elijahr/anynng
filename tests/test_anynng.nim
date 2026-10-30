# test_anynng.nim - High-Level anynng Public API Integration Tests
# Licensed under MIT License.

import std/unittest
import anynng

suite "anynng Public API & inproc:// Integration":
  test "PAIR0 inproc point-to-point messaging":
    let s1 = openPair0()
    let s2 = openPair0()

    let l = s1.listen("inproc://pair0_test")
    s2.dial("inproc://pair0_test")

    s2.send("hello from dialer")
    check s1.hasRecv()
    check s1.recvStr() == "hello from dialer"

    s1.send("hello from listener")
    check s2.hasRecv()
    check s2.recvStr() == "hello from listener"

    l.close()
    s2.close()

  test "PAIR1 inproc messaging with hop count":
    let s1 = openPair1()
    let s2 = openPair1()

    let l = s1.listen("inproc://pair1_test")
    s2.dial("inproc://pair1_test")

    s2.send("pair1 message")
    check s1.hasRecv()
    check s1.recvStr() == "pair1 message"

    l.close()
    s2.close()

  test "PUB0 and SUB0 inproc broadcast with topic filtering (WARN-02)":
    let pub = openPub0()
    let sub1 = openSub0()
    let sub2 = openSub0()

    let l = pub.listen("inproc://pubsub_test")

    # sub1 subscribes to "news."
    sub1.subscribe("news.")
    # sub2 subscribes to "alerts."
    sub2.subscribe("alerts.")

    sub1.dial("inproc://pubsub_test")
    sub2.dial("inproc://pubsub_test")

    pub.send("news.tech: anynng released")
    pub.send("alerts.critical: server reboot")

    check sub1.hasRecv()
    check sub1.recvStr() == "news.tech: anynng released"
    check not sub1.hasRecv() # sub1 should not get alerts

    check sub2.hasRecv()
    check sub2.recvStr() == "alerts.critical: server reboot"
    check not sub2.hasRecv() # sub2 should not get news

    l.close()
    sub1.close()
    sub2.close()

  test "REQ0 and REP0 inproc request-reply":
    let rep = openRep0()
    let req = openReq0()

    let l = rep.listen("inproc://reqrep_test")
    req.dial("inproc://reqrep_test")

    req.send("ping")
    check rep.hasRecv()
    let reqMsg = rep.recvMsg()
    check reqMsg.body == @[byte('p'), byte('i'), byte('n'), byte('g')]

    # REP replies (internally prepends request backtrace stack)
    rep.send("pong")
    check req.hasRecv()
    check req.recvStr() == "pong"

    l.close()
    req.close()

  test "PUSH0 and PULL0 inproc pipeline":
    let push = openPush0()
    let pull = openPull0()

    let l = pull.listen("inproc://pipeline_test")
    push.dial("inproc://pipeline_test")

    push.send("job-1")
    push.send("job-2")

    check pull.hasRecv()
    check pull.recvStr() == "job-1"
    check pull.hasRecv()
    check pull.recvStr() == "job-2"

    l.close()
    push.close()

  test "SURVEYOR0 and RESPONDENT0 inproc survey":
    let sur = openSurveyor0()
    let resp = openRespondent0()

    let l = resp.listen("inproc://survey_test")
    sur.dial("inproc://survey_test")

    sur.send("vote?")
    check resp.hasRecv()
    check resp.recvStr() == "vote?"

    resp.send("yes")
    check sur.hasRecv()
    check sur.recvStr() == "yes"

    l.close()
    sur.close()

  test "BUS0 inproc full mesh broadcast (CRIT-04)":
    let node1 = openBus0()
    let node2 = openBus0()
    let node3 = openBus0()

    let l1 = node1.listen("inproc://bus_test")
    node2.dial("inproc://bus_test")
    node3.dial("inproc://bus_test")

    node1.send("broadcast from node1")

    check node2.hasRecv()
    check node2.recvStr() == "broadcast from node1"

    check node3.hasRecv()
    check node3.recvStr() == "broadcast from node1"

    # CRIT-04: Node2 received message, but it does NOT re-broadcast to Node3!
    check not node3.hasRecv()

    l1.close()
    node2.close()
    node3.close()

  test "Negative controls":
    # SUB cannot send
    let sub = openSub0()
    expect(ValueError):
      sub.send("illegal send")

    # PULL cannot send
    let pull = openPull0()
    expect(ValueError):
      pull.send("illegal send")

    # Cannot subscribe on non-SUB socket
    let req = openReq0()
    expect(ValueError):
      req.subscribe("test")
