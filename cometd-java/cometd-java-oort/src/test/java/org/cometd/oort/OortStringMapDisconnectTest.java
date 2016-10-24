/*
 * Copyright (c) 2008-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cometd.oort;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.cometd.bayeux.server.ServerChannel;
import org.cometd.bayeux.server.ServerMessage;
import org.cometd.bayeux.server.ServerSession;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.cometd.server.AbstractService;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OortStringMapDisconnectTest extends OortTest {

    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private final List<Seti> setis = new ArrayList<>();
    private final List<OortStringMap<String>> oortStringMaps = new ArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private HttpClient httpClient;

    /**
     * Change this to change the test behaviour.
     * true :  The connects and logins will be done slower / with intervals to ensure this phase will not fail.
     *         This is helpful to ensure the disconnect phase is reached.
     * false : Everything in the connection phase will be done straight forward without intervals between the single connects or logins.
     *         The test might fail by that in this phase, so the disconnection phase might not be reached.
     */
    private boolean carefulConnectionPhase = true;


    public OortStringMapDisconnectTest(String serverTransport) {
        super(serverTransport);
    }

    @Before
    public void prepare() throws Exception {
        QueuedThreadPool clientThreads = new QueuedThreadPool(400);
        clientThreads.setName("client");
        httpClient = new HttpClient();
        httpClient.setExecutor(clientThreads);
        httpClient.setMaxConnectionsPerDestination(65536);
        httpClient.start();
    }

    @After
    public void dispose() throws Exception {
        for (OortStringMap<String> oortStringMap : oortStringMaps) {
            oortStringMap.stop();
        }
        if (httpClient != null) {
            httpClient.stop();
        }
        scheduler.shutdown();
    }


    @Test
    public void testMassiveDisconnect() throws Exception {
        int nodes = 4;

        int usersPerNode = 2000;
        int totalUsers = nodes * usersPerNode;
        // One event in a node is replicated to other "nodes" nodes.
        int totalEvents = nodes * totalUsers;

        prepareAndStartNodes(nodes);

        // Register a service so that when a user logs in,
        // it is recorded in the users OortStringMap.
        for (int i = 0; i < nodes; i++) {
            Seti seti = setis.get(i);
            OortStringMap<String> oortStringMap = oortStringMaps.get(i);
            new UserService(seti, oortStringMap);
        }

        final CountDownLatch presenceLatch = new CountDownLatch(totalEvents);
        Seti.PresenceListener presenceListener = new Seti.PresenceListener.Adapter() {
            @Override
            public void presenceRemoved(Event event) {
                presenceLatch.countDown();
                if (presenceLatch.getCount() % 100 == 0) {
                    LOG.info("presenceLatch.getCount() = " + presenceLatch.getCount());
                }
            }
        };

        for (int i = 0; i < nodes; i++) {
            Seti seti = setis.get(i);
            seti.addPresenceListener(presenceListener);
        }


        // register OortMap.EntryListeners

        final CountDownLatch putLatch = new CountDownLatch(totalEvents);
        final CountDownLatch removedLatch = new CountDownLatch(totalEvents);

        for (final OortStringMap<String> oortStringMap : oortStringMaps) {
            OortMap.EntryListener<String, String> listener = new OortMap.EntryListener.Adapter<String, String>() {
                @Override
                public void onPut(OortObject.Info<ConcurrentMap<String, String>> info, OortMap.Entry<String, String> entry) {
                    putLatch.countDown();
                    if (putLatch.getCount() % 100 == 0) {
                        LOG.info("putLatch.getCount() = " + putLatch.getCount());
                    }
                }

                @Override
                public void onRemoved(OortObject.Info<ConcurrentMap<String, String>> info, OortMap.Entry<String, String> entry) {
                    removedLatch.countDown();
                    if (removedLatch.getCount() % 100 == 0) {
                        LOG.info("removedLatch.getCount() = " + removedLatch.getCount());
                    }
                }
            };
            oortStringMap.addListener(new OortMap.DeltaListener<>(oortStringMap));
            oortStringMap.addEntryListener(listener);
        }


        // create client list per node
        List<List<BayeuxClient>> clients = new ArrayList<>();

        for (int n = 0; n < nodes; n++) {
            List<BayeuxClient> clientsPerNode = new ArrayList<>();
            clients.add(clientsPerNode);
        }


        // Do handshakes

        LOG.info("Starting clients (handshake)...");

        for (int c = 0; c < usersPerNode; c++) {
            for (int n = 0; n < nodes; n++) {
                if (carefulConnectionPhase) {
                    Thread.sleep(100L);
                }

                Oort oort = oorts.get(n);

                List<BayeuxClient> clientsPerNode = clients.get(n);
                BayeuxClient client = new BayeuxClient(oort.getURL(), scheduler, new LongPollingTransport(null, httpClient));
                clientsPerNode.add(client);
                client.handshake();

                if (c % 100 == 0) {
                    LOG.info("Handshaked client " + c + " on server " + n + ".");
                }
            }
        }

        LOG.info("Started clients (handshake)!");


        // Login users

        LOG.info("Login users...");

        for (int c = 0; c < clients.get(0).size(); c++) {
            for (int n = 0; n < nodes; n++) {
                if (carefulConnectionPhase) {
                    Thread.sleep(100L);
                }

                List<BayeuxClient> clientsPerNode = clients.get(n);
                BayeuxClient client = clientsPerNode.get(c);
                Assert.assertTrue(client.waitFor(60000, BayeuxClient.State.CONNECTED));
                String userName = "user_" + n + "_" + c;
                client.getChannel(UserService.LOGIN_CHANNEL).publish(userName);

                if (c % 100 == 0) {
                    LOG.info("Logged in user " + userName + " on server " + n + ".");
                }
            }
        }

        LOG.info("Logged in all users!");

        boolean putLatchCount = putLatch.await(totalEvents * 60L, TimeUnit.MILLISECONDS);
        Assert.assertTrue("putLatch has to be 0, but was " + putLatch.getCount(), putLatchCount);

        LOG.info("putLatch.getCount() after timeout = " + putLatch.getCount());


        // Thread.sleep(10000);

        checkOortMaps(usersPerNode);


        LOG.info("Disconnecting clients ... ");

        // Disconnect clients (kind of parallel on all nodes)
        for (int c = 0; c < clients.get(0).size(); c++) {
            for (int n = 0; n < nodes; n++) {
                List<BayeuxClient> clientsPerNode = clients.get(n);
                BayeuxClient client = clientsPerNode.get(c);
                client.disconnect();

                if (c % 100 == 0) {
                    LOG.info("Disconnecting client " + c + " on node " + n + ".");
                }
            }
        }

        LOG.info("Disconnect for all client triggered. Waiting for all disconnects to complete...");

        // wait until all clients are disconnected
        for (int c = 0; c < clients.get(0).size(); c++) {
            for (int n = 0; n < nodes; n++) {
                List<BayeuxClient> clientsPerNode = clients.get(n);
                BayeuxClient client = clientsPerNode.get(c);
                Assert.assertTrue(client.waitFor(60000, BayeuxClient.State.DISCONNECTED));
            }
        }

        LOG.info("Disconnected all clients!");

        boolean removedLatchCount = removedLatch.await(totalEvents * 60L, TimeUnit.MILLISECONDS);
        Assert.assertTrue("removedLatch has to be 0, but was " + removedLatch.getCount(), removedLatchCount);

        Thread.sleep(5000);

        checkOortMaps(0);

        boolean presenceLatchCount = presenceLatch.await(totalEvents * 60L, TimeUnit.SECONDS);
        Assert.assertTrue("presenceLatch has to be 0, but was " + presenceLatch.getCount(), presenceLatchCount);

        for (Seti seti : setis) {
            Assert.assertThat(seti.getUserIds().size(), Matchers.equalTo(0));
        }
    }

    private void checkOortMaps(int expectedSize){
        for (OortStringMap<String> oortStringMap : oortStringMaps) {
            // check all oortInfo objects (containing maps) in the oort map (the local and the remote objects)
            for (Iterator<OortObject.Info<ConcurrentMap<String, String>>> iter = oortStringMap.iterator(); iter.hasNext();) {
                OortObject.Info<ConcurrentMap<String, String>> oortInfo = iter.next();
                ConcurrentMap<String, String> dataMap = oortInfo.getObject();
                Assert.assertEquals(expectedSize, dataMap.size());
            }
            // ConcurrentMap<String, String> merge = oortStringMap.merge(OortObjectMergers.<String, String>concurrentMapUnion());
            // Assert.assertThat(merge.size(), Matchers.equalTo(expectedSize));
        }
    }

    private List<OortObject.Info<ConcurrentMap<String, String>>> getOortInfoAsList(OortStringMap<String> oortStringMap) {
        List<OortObject.Info<ConcurrentMap<String, String>>> oortInfosList = new ArrayList();
        for (Iterator<OortObject.Info<ConcurrentMap<String, String>>> iter = oortStringMap.iterator(); iter.hasNext(); ) {
            OortObject.Info<ConcurrentMap<String, String>> info = iter.next();
            oortInfosList.add(info);
        }
        return oortInfosList;
    }


    private void prepareAndStartNodes(int nodes) throws Exception {
        int edges = nodes * (nodes - 1);
        // Create the Oorts.
        final CountDownLatch joinLatch = new CountDownLatch(edges);
        Oort.CometListener joinListener = new Oort.CometListener.Adapter() {
            @Override
            public void cometJoined(Event event) {
                LOG.info("#### Comet joined the cluster " + event.getCometURL() + " (cometId " + event.getCometId()  + ")");
                joinLatch.countDown();
                LOG.info("joinLatch.getCount() = " + joinLatch.getCount());
            }
            public void cometLeft(Event event) {
                LOG.info("#### Comet left the cluster " + event.getCometURL() + " (cometId " + event.getCometId()  + ")");
            }
        };
        Map<String, String> options = new HashMap<>();
        options.put("ws.maxMessageSize", String.valueOf(1024 * 1024));
        for (int i = 0; i < nodes; i++) {
            LOG.info("Starting server " + i);
            Server server = startServer(0, options);
            Oort oort = startOort(server);
            oort.addCometListener(joinListener);
        }
        // Connect the Oorts.
        Oort oort1 = oorts.get(0);
        for (int i = 1; i < oorts.size(); i++) {
            Oort oort = oorts.get(i);
            OortComet oortComet1X = oort1.observeComet(oort.getURL());
            Assert.assertTrue(oortComet1X.waitFor(600000, BayeuxClient.State.CONNECTED));
            OortComet oortCometX1 = oort.findComet(oort1.getURL());
            Assert.assertTrue(oortCometX1.waitFor(600000, BayeuxClient.State.CONNECTED));
        }

        boolean joinLatchCount = joinLatch.await(nodes * 600, TimeUnit.SECONDS);
        Assert.assertTrue("joinLatch has to be 0, but was " + joinLatch.getCount(), joinLatchCount);

        // Thread.sleep(1000);

        int startEvents = 0;
        for (int i = nodes; i > 0; --i) {
            startEvents += i;
        }

        // Start the Setis.
        final CountDownLatch setiLatch = new CountDownLatch(startEvents);
        for (final Oort oort : oorts) {
            oort.getBayeuxServer().createChannelIfAbsent("/seti/all").getReference().addListener(new ServerChannel.MessageListener() {
                @Override
                public boolean onMessage(ServerSession from, ServerChannel channel, ServerMessage.Mutable message) {
                    if (message.getDataAsMap().get("alive") == Boolean.TRUE) {
                        setiLatch.countDown();
                        LOG.info("setiLatch.getCount() = " + setiLatch.getCount());
                    }
                    return true;
                }
            });
            Seti seti = new Seti(oort);
            setis.add(seti);
            seti.start();
        }

        boolean setiLatchCount = setiLatch.await(nodes * 600, TimeUnit.SECONDS);
        Assert.assertTrue("setiLatch has to be 0, but was " + setiLatch.getCount(), setiLatchCount);

        // Start the OortStringMaps.
        String name = "users";
        OortObject.Factory<ConcurrentMap<String, String>> factory = OortObjectFactories.forConcurrentMap();
        final CountDownLatch mapLatch = new CountDownLatch(startEvents);
        for (Oort oort : oorts) {
            OortStringMap<String> users = new OortStringMap<>(oort, name, factory);
            oortStringMaps.add(users);

            users.addListener(new OortObject.Listener.Adapter<ConcurrentMap<String, String>>() {
                @Override
                public void onUpdated(OortObject.Info<ConcurrentMap<String, String>> oldInfo, OortObject.Info<ConcurrentMap<String, String>> newInfo) {
                    if (oldInfo == null) {
                        mapLatch.countDown();
                        LOG.info("mapLatch.getCount() = " + mapLatch.getCount());
                    }
                }
            });
            users.start();
        }

        boolean mapLatchCount = mapLatch.await(nodes * 600, TimeUnit.SECONDS);
        Assert.assertTrue("mapLatch has to be 0, but was " + mapLatch.getCount(), mapLatchCount);

        // Verify that the OortStringMaps are setup correctly.
        final String setupKey = "setup";
        final CountDownLatch setupLatch = new CountDownLatch(2 * nodes);
        OortMap.EntryListener<String, String> setupListener = new OortMap.EntryListener.Adapter<String, String>() {
            @Override
            public void onPut(OortObject.Info info, OortMap.Entry entry) {
                if (entry.getKey().equals(setupKey)) {
                    setupLatch.countDown();
                    LOG.info("setupLatch.getCount() = " + setupLatch.getCount());
                }
            }

            @Override
            public void onRemoved(OortObject.Info<ConcurrentMap<String, String>> info, OortMap.Entry<String, String> entry) {
                if (entry.getKey().equals(setupKey)) {
                    setupLatch.countDown();
                    LOG.info("setupLatch.getCount() = " + setupLatch.getCount());
                }
            }
        };
        for (OortStringMap<String> oortStringMap : oortStringMaps) {
            oortStringMap.addEntryListener(setupListener);
        }
        OortStringMap<String> oortStringMap1 = oortStringMaps.get(0);
        OortObject.Result.Deferred<String> putAction = new OortObject.Result.Deferred<>();
        oortStringMap1.putAndShare(setupKey, setupKey, putAction);
        Assert.assertNull(putAction.get(15, TimeUnit.SECONDS));
        OortObject.Result.Deferred<String> removeAction = new OortObject.Result.Deferred<>();
        oortStringMap1.removeAndShare(setupKey, removeAction);
        Assert.assertNotNull(removeAction.get(15, TimeUnit.SECONDS));

        boolean setupLatchCount = setupLatch.await(nodes * 600, TimeUnit.SECONDS);
        Assert.assertTrue("setupLatch has to be 0, but was " + setupLatch.getCount(), setupLatchCount);
    }


    public static class UserService extends AbstractService implements ServerSession.RemoveListener {
        private static final String LOGIN_CHANNEL = "/service/login";
        private final Seti seti;
        private final OortStringMap<String> oortStringMap;

        public UserService(Seti seti, OortStringMap<String> oortStringMap) {
            super(seti.getOort().getBayeuxServer(), "userService");
            this.seti = seti;
            this.oortStringMap = oortStringMap;
            addService(LOGIN_CHANNEL, "login");
        }

        public void login(ServerSession session, ServerMessage message) {
            session.addListener(this);
            String userName = (String)message.getData();
            session.setAttribute("userName", userName);
            seti.associate(userName, session);
            oortStringMap.putAndShare(userName, userName, new OortObject.Result.Deferred<String>());
        }

        @Override
        public void removed(ServerSession session, boolean timeout) {
            String userName = (String)session.getAttribute("userName");
            seti.disassociate(userName, session);
            oortStringMap.removeAndShare(userName, new OortObject.Result.Deferred<String>());
        }
    }
}
