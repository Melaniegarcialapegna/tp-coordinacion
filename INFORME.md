# Informe: Coordinación de Sum/Aggregation y escalabilidad del sistema
### Melanie Garcia Lapegna ~ 111848

Para dar un poco de contexto, la idea del flujo es que el pipeline procesa por cliente `(fruta,cantidad)` y devuelve el **top K** de frutas.
El computo esta distribuido en distintas etapas `gateway -> sum -> aggregation -> join`, y las etapas de `sum` y `aggregation` pueden contar con multiples réplicas trabajando de manera paralela sobre los datos de un mismo cliente (la cantidad de replicas es configurable).

## Coordinación entre instancias de Sum

Las distintas instancias de `sum` consumen de la cola `INPUT_QUEUE`. Cada replica va manteniendo su propio "acumulador" parcial por cliente dentro de un diccionario (`{client_id: {fruta: FruitItem}}`).

El problema inicial era que el mensaje de EOF de un cliente le puede llegar a **cualquier instancia de `sum`** y esa instancia no tenia forma de saber si las demas ya terminaron de procesar los registros que le llegaron de ese cliente, ni tiene las sumas parciales de las otras replicas.

Para resolver esto se agrego un **exchange `SUM_CONTROL_EXCHANGE`** de tipo `direct`al que todas las instancias de `sum` estan suscriptas.

El flujo implementado es el siguiente:
- El `MessageHandler` del gateway cuenta cuantos registros mando cada cliente y adjunta esta cantidad en el mensaje de EOF.
- La replica que recibe el EOF hace _broadcast_ de `(cliente_id,total_esperado)` a todas las instancias por el exchange de control.
- Cada instancia al recibir ese aviso reporta al cuantos registros de ese cliente proceso hasta el momento (usando el contador `items_processed_by_clients_before_eof`) tambien por el mismo excahnge de control.
- Todas las replicas van acumulando esos conteos, en caso de que aun no se haya llegado al `total_esperado` se siguen esperando la llegada de esos items sumandolos. Asi hasta llegar a ese total. 
- Una vez que la suma iguala al total esperado cada una de las instancias llama a `_process_eof` para ese cliente el cual se encarga de enviar las sumas parciales a la siguiente etapa.

## Coordinación entre instancias de Aggregation

Como se menciono anteriormente, cada `sum` particiona las frutas ya sumadas de un cliente entre las distintas instancias de `aggretation`.
Esto se hace usando un **hash deterministico** para que cada fruta caiga siempre en la misma instancia de aggregation. O sea, basicamente la idea es que cada replica de aggregation se encarga de manejar los top parciales de algunas frutas en particular (y este hasheo se realiza en `sum`).
Cuando `sum` termina con un cliente hace el _broadcast_ del EOF a todas las instancias de `aggregation` porque cada una de estas necesita saber que ese `sum` ya no le va a mandar mas datos de ese cliente. Entonces, cada replica de `aggregation` espera recibir el EOF de las `SUM_AMOUNT` instancias de sum antes de calcular el top parcial para ese cliente.

Una vez que la instancia de `aggregation` recibio los `SUM_AMOUNT` EOFs, se **calcula el topK de su propio subconjunto de frutas** (utilizando un heap, explicado mas adelante) y lo manda a la siguiente fase que es la del `join` como un "top parcial".

En el `join` se reciben los top parciales y en base a ellos se calcula el **top K final**.

#### Utilizacion de `heapq.nlargest` en vez de mantener una lista ordenada por cada mensaje que llega.

En la version original se utilizaba `bisect.insort`. Pero en esta implementacion con `m` mensajes y `n` frutas distintas por cliente, cada mensaje costaria `O(n)` por lo que el costo final por procesar los datos de un cliente seria `O(m * n)`.

Esta complejidad es innecesaria ya que nadie necesita el top ordenado hasta que llegue el EOF.

En la implementación propuesta la complejidad final es `O(m) + O(n log TOP_SIZE)` (y como `TOP_SIZE` es una cte podria decirse que es `O(m)+ O(n)`).

Esto se logro haciendo que:
- Por cada mensaje que se procesa, unicamente se realizan **operaciones constantes (`O(1)`)** en vez de `O(m * n)`.
- Y luego con `heapq.nlargest` no se ordenan las `n` frutas (lo cual seria `O(n log n)` si se usara un sort por ejemplo). Sino que se arma un heap de tamaño fijo `TOP_SIZE` y se lo recorre una sola vez sobre las `n` frutas, quedando en `O(n log TOP_SIZE)`. Y como ya se menciono, como `TOP_SIZE` es una constante chica, podria decirse que es casi lineal en `n`.

## Escalabilidad

### Respecto cantidad de clientes
Cada una de las etapas guarda los datos por cliente (usando diccionarios que tienen como clave `client_id`), haciendo esto los clientes no se "acoplan".

### Respecto a grandes volumenes de datos
Como por cada cliente se tiene `{client_id :{fruit : ItemFruit}}` no importa si le llegan tres o miles de registros de "frutilla" porque se termina conviertiendo en un unico ItemFruit.
Y ademas, se tienen varias instancias de `sum` y/o `aggregation` trabajando en paralelo, si el volumen de datos crece se podrian crear aun mas instancias para repartir aun mejor a carga sin cambiar la logica.

### Respecto a la cantidad de mensajes que se utilizan para comunicarse entre las distintas instancias de sum
Los mensajes de coordinación entre las instancias de `sum` no dependen de cuantos registros envie cada cliente sino de cuantas instancias de `sum` existan.
El flujo es:
- Cuando un cliente termina, cada replica de `sum` envia el conteo de la cantidad de items que recibio hasta el momento, esto lo hace con un **unico mensaje** sin importar si son cuatro o miles.
- Despues, cada instancia de `sum` le avisa a cada replica de `aggregation` que ya terminó. Por lo que, la cantidad de estos mensajes de aviso dependen unicamente de la cantidad de instancias de `sum` y `aggregation` que hayan, no del tamaño de datos.

Ademas, se opto por recien arrancar a avisar sobre la cantidad de items que llegaron de para un cliente **una vez llegado el EOF**, por lo que ese transito de mensajes arranca una vez que el `gateway` termino de enviar todos los registros, por lo que no deberian ser muchos.