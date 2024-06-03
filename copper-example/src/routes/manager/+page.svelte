<script>
  // @ts-nocheck

  import { invoke } from "@tauri-apps/api";
  import Modal from "../../components/modal.svelte";

  let library = new URLSearchParams(window.location.search).get("name");
  let books = [];

  let isLoading = false;
  let showAdd = false;
  let showDebug = false;
  let showSql = false;

  let newTitle = "";
  let newAuthor = "";
  let newYear = "";
  let newQuantity = "";

  let debugTextAreaText = "";

  let sqlTextAreaText = "";
  let query = "";

  async function createBook() {
    if (isLoading) return;
    isLoading = true;
    await invoke("new_book", {
      shop: library,
      name: newTitle,
      author: newAuthor,
      year: newYear,
      quantity: newQuantity,
    });
    showAdd = false;
    newTitle = "";
    newAuthor = "";
    newYear = "";
    newQuantity = "";
    await fetchBooks();
    isLoading = false;
  }

  async function fetchBooks() {
    let books_str = await invoke("fetch_book", { shop: library });
    books = books_str.map((jsonString) => JSON.parse(jsonString));
    books = books.sort((a, b) => a.name.localeCompare(b.name));
    isLoading = false;
  }

  async function increaseBookQuantity(name) {
    if (isLoading) return;
    isLoading = true;
    await invoke("add_book", { shop: library, name });
    await fetchBooks();
    isLoading = false;
  }

  async function decreaseBookQuantity(name) {
    console.log("Removing from " + name);
    if (isLoading) return;
    isLoading = true;
    await invoke("sell_book", { shop: library, name });
    await fetchBooks();
    isLoading = false;
  }

  async function removeBook(name) {
    if (isLoading) return;
    isLoading = true;
    await invoke("remove_book", { shop: library, name });
    await fetchBooks();
    isLoading = false;
  }

  async function showDebugLsmTree() {
    if (isLoading) return;
    isLoading = true;
    debugTextAreaText = await invoke("debug_print", { shop: library });
    isLoading = false;
  }

  async function showDebugLogs() {
    if (isLoading) return;
    isLoading = true;
    debugTextAreaText = await invoke("get_log", { shop: library });
    isLoading = false;
  }

  async function executeSqlQuery() {
    sqlTextAreaText =
      "QUERY:\n" +
      query.toUpperCase() +
      "\n\n" +
      "RESULT:\n" +
      (await invoke("execute_sql", { shop: library, query }));
    fetchBooks();
  }

  fetchBooks();
</script>

<div class="h-full w-full flex flex-col">
  <!--SQL Playground modal-->
  <Modal bind:visible={showSql}>
    <h1>SQL Playground</h1>
    <input type="text" bind:value={query} placeholder="SQL Query" />
    <button
      class="green-button !w-full"
      disabled={isLoading}
      on:click={() => executeSqlQuery()}>Execute</button
    >
    <textarea readonly="true" bind:value={sqlTextAreaText} class="h-[400px]"
    ></textarea>
    <p class="text-center">
      Available queries:<br /> SELECT / INSERT / DELETE / UPDATE
    </p>
    <button class="neutral-button float-end" on:click={() => (showSql = false)}
      >Close</button
    >
  </Modal>

  <!--Debug modal-->
  <Modal bind:visible={showDebug}>
    <h1>Debug Panel</h1>
    <textarea readonly="true" bind:value={debugTextAreaText} class="h-[400px]"
    ></textarea>
    <div class="flex space-x-4 justify-end">
      <button
        class="neutral-button float-end"
        on:click={() => (showDebug = false)}>Close</button
      ><button
        class="neutral-button float-end"
        on:click={() => showDebugLsmTree()}>LSM Tree</button
      ><button class="neutral-button float-end" on:click={() => showDebugLogs()}
        >Logs</button
      >
    </div>
  </Modal>

  <!--New book modal-->
  <Modal bind:visible={showAdd}>
    <h1>Add a book</h1>
    <input type="text" bind:value={newTitle} placeholder="Title" />
    <input type="text" bind:value={newAuthor} placeholder="Author" />
    <input type="text" bind:value={newYear} placeholder="Publication year" />
    <input type="text" bind:value={newQuantity} placeholder="Quantity" />
    <div class="flex space-x-4 justify-end">
      <button class="neutral-button" on:click={() => (showAdd = false)}
        >Close</button
      >
      <button
        class="green-button"
        disabled={isLoading}
        on:click={() => createBook()}>Create</button
      >
    </div>
  </Modal>

  <!--Top menu-->
  <div class="menu">
    <div class="flex items-center">
      <img
        src="/blue_book_3d.png"
        alt="Blue Book"
        class="h-8 w-8 inline mr-2"
      />
      <h1>Library {library}</h1>
    </div>
    <button on:click={() => (showAdd = true)} class="green-button"
      >New book</button
    >
  </div>

  <!--List-->
  <div class="flex-1 space-y-4 bg-[url(/wallpaper.png)] bg-cover">
    <table>
      <tr>
        <th>Title</th>
        <th>Author</th>
        <th>Year</th>
        <th>Quantity</th>
        <th class="w-[280px]">Actions</th>
      </tr>
      {#each books as book, index}
        <tr class="text-white" key={index}>
          <td>{book.name}</td>
          <td>{book.author}</td>
          <td>{book.year}</td>
          <td>{book.quantity}</td>
          <td
            ><div class="flex space-x-4 w-full py-1">
              <button
                on:click={() => decreaseBookQuantity(book.name)}
                class="neutral-button flex-1/3">Sell 1</button
              >
              <button
                on:click={() => increaseBookQuantity(book.name)}
                class="green-button flex-1/3">Add 1</button
              >
              <button
                on:click={() => removeBook(book.name)}
                class="red-button flex-1/3">Delete</button
              >
            </div></td
          >
        </tr>
      {/each}
    </table>
  </div>

  <!--Bottom menu-->
  <div class="menu">
    <a href="/" class="neutral-button">Return</a>
    <div class="space-x-4">
      <button
        on:click={() => {
          query = "";
          sqlTextAreaText = "";
          showSql = true;
        }}
        class="neutral-button">SQL Playground</button
      >
      <button on:click={() => (showDebug = true)} class="neutral-button"
        >Debug Panel</button
      >
    </div>
  </div>
</div>
