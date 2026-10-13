# Kubernetes example

Plain manifests that run descry in a cluster, for clusters that do not have the
Prometheus Operator's CRDs (or any CRDs) installed. One Deployment, one
ConfigMap, a kustomization that ties them together, and an opt-in
NetworkPolicy outside the kustomization. The Prometheus alerting
rules live in [`contrib/prometheus/`](../../contrib/prometheus/).

| File | What it is |
|---|---|
| `kustomization.yaml` | resources, the image tag, and a label on every object |
| `deployment.yaml` | one replica, `--listen=:9090`, health probes, a locked-down security context, no service account token, a memory limit with `GOMEMLIMIT` under it, and a CPU request but no CPU limit (throttling would inflate the latency descry measures) |
| `configmap.yaml` | the descry config, mounted at `/etc/descry/config.yaml` |
| `networkpolicy.yaml` | opt-in, not in the kustomization: who may reach the metrics port, and where descry may connect (see [Restrict traffic](#restrict-traffic-with-a-networkpolicy)) |

The config has two targets, chosen so the first run shows both outcomes:

- `https://example.com` is expected **up**.
- `https://kubernetes.default.svc.cluster.local/` (the kube-apiserver Service)
  is expected **down** with error class `ssrf_blocked` on kind. It resolves to
  a private ClusterIP there, and descry's dial guard refuses private addresses.

Whether the guard refuses in-cluster Services depends on the cluster's Service
range. Where it is a private range (the default for kind, kubeadm and EKS), a
ClusterIP is refused with `ssrf_blocked`. Some clusters allocate Service IPs
from a public range, GKE's default `34.118.224.0/20` for example. There the
guard does not refuse them: the API server target is dialled and fails with
`tls_error` (its certificate is signed by the cluster CA), so on such a cluster
the output of steps 5 and 6 below shows that class instead, and an in-cluster
Service over plain `http://` on port 80 is reachable. The guard is best-effort,
not a security boundary; if descry must not reach in-cluster Services, enforce
that with a NetworkPolicy. `networkpolicy.yaml` does not cover this case as
shipped: add the cluster's Service and pod ranges to its except lists first
(see [Restrict traffic](#restrict-traffic-with-a-networkpolicy), and its
caveat about Service addresses).

Events go to the pod log (`sink: stdout`), because the container's root
filesystem is read-only.

Things to know before you adapt it:

- **One replica.** descry has no leader election. A second replica probes every
  target again and doubles every event and series. For the same reason the
  Deployment uses the `Recreate` strategy, so a rollout stops the old pod
  before the new one starts. Outside a rollout a Deployment does not promise
  at most one pod: after an eviction, or while a node is unreachable, the
  replacement can probe alongside the old pod for a while.
- **A restart can log a false `down`.** On SIGTERM descry cancels the checks
  in flight, and a cancelled check is still published as a `down` event, so a
  rollout, eviction or drain can leave one such event per target that was
  mid-check.
- **Only ports 80 and 443.** The guard also refuses any other port in a target
  URL, with the same `ssrf_blocked` class, so `https://host:8443/` is reported
  down without being dialled.
- **The metrics port is open to the cluster.** `--listen=:9090` binds every
  interface of the pod and the endpoints have no authentication. The probes
  use the same listener, which serves at most 64 connections at once, so a
  client that holds those open can make the kubelet's HTTP readiness probe on
  `/readyz` time out and mark the pod unready. The liveness probe is a TCP
  connect, which the kernel completes from its accept backlog while the slots
  are full, so holding the slots alone does not get the pod restarted. A
  client that also fills the backlog (a few thousand more connections) makes
  the TCP probe time out too, so the probe choice only raises the bar.
  Restrict who can reach the port (the kubelet and your scraper) with a
  NetworkPolicy if the cluster has untrusted workloads; `networkpolicy.yaml`
  is a starting point. See
  [Metrics and health endpoints](../../docs/OPERATIONS.md#metrics-and-health-endpoints).
- **No egress proxy.** descry ignores `HTTP_PROXY` and `HTTPS_PROXY`, so its
  dial guard sees the real target address. Targets need direct egress; where
  the cluster only reaches the internet through a proxy, the `example.com`
  target reports down.
- **Config changes need a restart.** The ConfigMap is read at startup:
  after editing it and applying, run `kubectl rollout restart deployment/descry`.

## Restrict traffic with a NetworkPolicy

`networkpolicy.yaml` is opt-in and not listed in `kustomization.yaml`, because
it has to be adapted first. Apply it after the rest:

```bash
kubectl apply -k examples/kubernetes
kubectl apply -f examples/kubernetes/networkpolicy.yaml
```

or list it under `resources` in your own kustomization. It selects the descry
pod and does three things:

- **Ingress: only the scraper reaches port 9090.** The admin server is
  unauthenticated, its `url` labels name every target, and a client that holds
  its connection slots stalls the readiness probe. Set the `namespaceSelector`
  and `podSelector` marked `ADAPT` to your Prometheus (the file assumes
  namespace `monitoring` and pods labelled
  `app.kubernetes.io/name: prometheus`). The kubelet's probes come from the
  node, and most CNIs let node traffic through regardless of policy; check
  that yours does, or the pod goes unready once the policy is applied.
- **Egress: TCP 80 and 443 outside the special-purpose ranges.** Every range
  descry's dial guard blocks is excepted: for IPv4 the private, shared
  (`100.64.0.0/10`), loopback, link-local, documentation, multicast, reserved
  and other special-purpose ranges, and for IPv6 unspecified, loopback,
  unique-local, link-local and multicast. NAT64 (`64:ff9b::/96`) is excepted
  as well, though the dial guard allows it. This is defense in depth
  beside descry's own dial guard, not a replacement for it. The lists do not
  know your cluster's own ranges: where the Service or pod CIDR is public
  (GKE's default Service range `34.118.224.0/20`, say), append both to the
  except lists, or descry can still reach in-cluster Services and pods on
  ports 80 and 443. Whether an `ipBlock` applies to traffic for a ClusterIP is
  CNI-specific: on kind, a pod under this policy still reaches the API server
  through its Service, whose endpoint is the node's address.
- **Egress: DNS to the kube-dns pods only.** Adapt the selector if your
  cluster DNS pods are not labelled `k8s-app: kube-dns`. With NodeLocal
  DNSCache, DNS is answered by a cache on the node, which this rule does not
  match, so DNS fails. Pods query the address in their `/etc/resolv.conf`:
  usually the kube-dns Service's ClusterIP, which the node cache also listens
  on with kube-proxy in iptables mode, or `169.254.20.10` where kubelet's
  `clusterDNS` points there (required with IPVS mode).
  Uncomment the commented rule in the file and set it to that address.

The policy does nothing on a cluster whose CNI does not enforce
NetworkPolicy. kind's default CNI does enforce it.

## Try it on a kind cluster

The release workflow publishes `ghcr.io/fairbearlab/descry` from v0.5.0 on,
and `kustomization.yaml` pins v0.5.0, so running that release takes only
`kubectl apply -k examples/kubernetes` (see the note after step 3). The steps
below build an image from this checkout instead and load it straight into
kind, so they run your own changes. You need Docker,
[kind](https://kind.sigs.k8s.io/), `kubectl` and Go. Run everything from the
root of the repository.

1. Create a cluster.

   ```bash
   kind create cluster --name descry-example
   ```

2. Build the image with [ko](https://ko.build/) and load it into the cluster.
   With `KO_DOCKER_REPO=kind.local`, ko loads the image into the kind cluster
   named by `KIND_CLUSTER_NAME` instead of pushing it. `--platform` matches the
   cluster's architecture to your machine's.

   ```bash
   KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME=descry-example \
     go run github.com/google/ko@v0.19.1 build \
       --base-import-paths --platform="linux/$(go env GOARCH)" --tags dev ./cmd/descry
   ```

   The last line of the output is the loaded image, tagged with its
   digest (`kind.local/descry:<hex digest>`); `--base-import-paths` and
   `--tags dev` also tag it `kind.local/descry:dev`, which is the name the next
   step uses. ko's "git is in a dirty state" warning is harmless here.

3. Point the manifests at that image without editing the checked-in
   kustomization: apply them through a throwaway overlay in a temporary
   directory.

   ```bash
   overlay="$(mktemp -d)"
   cp -R examples/kubernetes "$overlay/base"
   printf '%s\n' \
     'resources:' \
     '  - base' \
     'images:' \
     '  - name: ghcr.io/fairbearlab/descry' \
     '    newName: kind.local/descry' \
     '    newTag: dev' \
     > "$overlay/kustomization.yaml"
   kubectl apply -k "$overlay"
   kubectl rollout status deployment/descry --timeout=120s
   ```

   With a published release image, `kubectl apply -k examples/kubernetes`
   is all you need, with `newTag` in `kustomization.yaml` set to the release
   you want (v0.5.0 or later, with the `v`). Verify it first as the main
   README's [Verify a release](../../README.md#verify-a-release) shows, and
   for a real deployment pin the digest you verified: `digest:` in place of
   `newTag` in the kustomization, since a tag can be moved.

4. Port-forward the metrics port. This holds the terminal, so run the next step
   in another one.

   ```bash
   kubectl port-forward deployment/descry 9090:9090
   ```

5. Wait about 30 seconds: a target's first check fires up to one interval
   (10s here) after start, and the two targets have different offsets. Then
   check the probes and the metrics.

   ```bash
   curl -s localhost:9090/healthz   # ok
   curl -s localhost:9090/readyz    # ok
   curl -s localhost:9090/metrics | grep -E '^descry_check_(up|errors_total)'
   ```

   The `descry_check_errors_total` grep also prints a `0` line for every other
   error class; ignore those. Two kinds of lines show the example worked.
   `example.com` is up:

   ```text
   descry_check_up{url="https://example.com/"} 1
   ```

   and the API server was refused by the dial guard, which counts under the
   `ssrf_blocked` class and leaves that target's `descry_check_up` at 0:

   ```text
   descry_check_errors_total{class="ssrf_blocked"} 4
   descry_check_up{url="https://kubernetes.default.svc.cluster.local/"} 0
   ```

   The `url` label is the target URL reduced to scheme, host and path, with an
   empty path written as `/`, so `https://example.com` is labelled
   `https://example.com/`. The `ssrf_blocked` count is above 0 and grows by one
   per check of the API server target; the exact number depends on how long the
   pod has been running.

6. Read the events, one CloudEvent per check, in the pod log. The API server
   target is `down`, with `"error_class":"ssrf_blocked"`; `example.com` is `up`.

   ```bash
   kubectl logs deployment/descry
   ```

7. Clean up. Stop the port-forward first (Ctrl-C).

   ```bash
   kind delete cluster --name descry-example
   rm -rf "$overlay"
   ```

## Validate the manifests

[kubeconform](https://github.com/yannh/kubeconform) checks the rendered output
against the Kubernetes JSON schemas, and `-strict` rejects fields the schema does
not know, so a misspelled field fails instead of being dropped:

```bash
set -o pipefail
schemas='https://raw.githubusercontent.com/yannh/kubernetes-json-schema/817113f21a2643311cbc3e992bb44ffe1c416c54/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json'
kubectl kustomize examples/kubernetes \
  | kubeconform -strict -summary -kubernetes-version 1.37.1 -schema-location "$schemas"
kubeconform -strict -summary -kubernetes-version 1.37.1 -schema-location "$schemas" \
  examples/kubernetes/networkpolicy.yaml
```

kubeconform downloads the schemas as it runs. Without `-schema-location` it
reads them from the moving `master` branch of
[kubernetes-json-schema](https://github.com/yannh/kubernetes-json-schema);
`$schemas` pins them to one commit of it, and `-kubernetes-version` picks the
schema directory within that commit. The CI `manifests` job runs this same
pipeline with pinned `kubectl` and `kubeconform` binaries and the same schema
commit, so a manifest that no longer renders or no longer validates fails the
build. The NetworkPolicy is not in the kustomization, so it is validated on its
own. `pipefail` makes a failure in either stage fail the pipeline.

kubeconform checks the ConfigMap's shape, not the descry config inside it.
`go test ./cmd/descry/` covers that: it loads the embedded `config.yaml` with
descry's own config checks, as the Deployment runs it (`--listen` with
per-target series on), so a renamed config field or a shared metrics label
fails the build instead of crash-looping the pod.
