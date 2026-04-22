function setupSupplierShortcut() {
  const select = document.querySelector("#material-supplier-select");
  if (!select) {
    return;
  }

  select.addEventListener("change", () => {
    if (select.value === "__new__") {
      rememberPendingSelection(select);
      select.value = "";
      select.dispatchEvent(new Event("change", { bubbles: true }));
      saveFormDraft(select.form);
      window.location.href = select.dataset.newSupplierUrl;
    }
  });
}

function buildShortcutReturnUrl(select, rawUrl) {
  if (!rawUrl) {
    return rawUrl;
  }

  const url = new URL(rawUrl, window.location.origin);
  const currentPath = `${window.location.pathname}${window.location.search}`;
  const isJobForm = select.form?.classList?.contains("job-form");
  const isJobCreationTarget =
    url.pathname === "/products" ||
    url.pathname === "/registry/customers" ||
    url.pathname === "/registry/payment-terms" ||
    url.pathname === "/registry/sales-channels";

  if (isJobForm && isJobCreationTarget) {
    url.searchParams.set("return_to", currentPath);
  }

  return `${url.pathname}${url.search}${url.hash}`;
}

function setupSelectShortcuts(root = document) {
  const selects = Array.from(root.querySelectorAll("select[data-new-url]"));
  selects.forEach((select) => {
    const createOptions = Array.from(select.options).filter((option) =>
      option.value.startsWith("__new__")
    );
    createOptions
      .reverse()
      .forEach((option) => select.insertBefore(option, select.firstChild));
    if (select.dataset.selectShortcutReady === "1") {
      return;
    }
    select.dataset.selectShortcutReady = "1";
    select.addEventListener("change", () => {
      if (select.value === "__new__") {
        rememberPendingSelection(select);
        select.value = "";
        select.dispatchEvent(new Event("change", { bubbles: true }));
        saveFormDraft(select.form);
        window.location.href = buildShortcutReturnUrl(select, select.dataset.newUrl);
      }
    });
  });
}

const FORM_DRAFT_PREFIX = "codex-form-draft:";
const PENDING_SELECTION_PREFIX = "codex-pending-selection:";
const SCROLL_RESTORE_KEY = "codex-scroll-restore";
const MATERIALS_CATALOG_PANEL_ID = "materials-catalog-panel";
const MATERIALS_FILTER_FIELDS = [
  "sku",
  "material_type",
  "color",
  "name",
  "manufacturer_name",
  "lot_number",
  "location",
];

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function getFormIndex(form) {
  return Array.from(document.querySelectorAll("form")).indexOf(form);
}

function getPendingSelectionKey(form) {
  return `${PENDING_SELECTION_PREFIX}${window.location.pathname}::${getFormIndex(form)}`;
}

function getSelectSelectionIndex(select) {
  const form = select.form;
  if (!form) {
    return 0;
  }
  let selector = "";
  if (select.classList.contains("product-picker")) {
    selector = "select.product-picker";
  } else if (select.id === "material-supplier-select") {
    selector = "select#material-supplier-select";
  } else if (select.name) {
    selector = `select[name="${select.name}"]`;
  }
  if (!selector) {
    return 0;
  }
  return Array.from(form.querySelectorAll(selector)).indexOf(select);
}

function rememberPendingSelection(select) {
  if (!select?.form || !window.sessionStorage) {
    return;
  }

  let queryKey = "";
  if (select.classList.contains("product-picker")) {
    queryKey = "selected_product_id";
  } else if (select.id === "material-supplier-select") {
    queryKey = "selected_supplier_id";
  } else {
    const keyMap = {
      customer_id: "selected_customer_id",
      material_id: "selected_material_id",
      component_id: "selected_component_id",
      payment_terms: "selected_payment_term",
      sale_channel: "selected_sale_channel",
      supplier_id: "selected_supplier_id",
    };
    queryKey = keyMap[select.name] || "";
  }

  if (!queryKey) {
    return;
  }

  window.sessionStorage.setItem(
    getPendingSelectionKey(select.form),
    JSON.stringify({
      queryKey,
      name: select.name || "",
      id: select.id || "",
      isProductPicker: select.classList.contains("product-picker"),
      index: Math.max(getSelectSelectionIndex(select), 0),
    })
  );
}

function getDraftFieldKey(field) {
  if (field.classList?.contains("date-mask-field") && field.dataset.dateField) {
    return `date:${field.dataset.dateField}`;
  }
  return field.name || "";
}

function getVisibleDraftFields(form) {
  return Array.from(
    form.querySelectorAll("input[name], select[name], textarea[name], .date-mask-field")
  ).filter((field) => {
    if (field.disabled) {
      return false;
    }
    if (field.classList?.contains("date-mask-field")) {
      return true;
    }
    if (!field.name) {
      return false;
    }
    if (field.type === "hidden" || field.type === "file" || field.type === "submit") {
      return false;
    }
    return true;
  });
}

function getFormDraftKey(form) {
  return `${FORM_DRAFT_PREFIX}${window.location.pathname}::${getFormIndex(form)}`;
}

function saveFormDraft(form) {
  if (!form || !window.sessionStorage) {
    return;
  }
  const state = { fields: {} };
  getVisibleDraftFields(form).forEach((field) => {
    const key = getDraftFieldKey(field);
    if (!key) {
      return;
    }
    if (!state.fields[key]) {
      state.fields[key] = [];
    }
    if (field.type === "checkbox") {
      state.fields[key].push(field.checked ? "1" : "0");
      return;
    }
    state.fields[key].push(field.value);
  });
  window.sessionStorage.setItem(getFormDraftKey(form), JSON.stringify(state));
}

function clearFormDraft(form) {
  if (!form || !window.sessionStorage) {
    return;
  }
  window.sessionStorage.removeItem(getFormDraftKey(form));
}

function setupFormDraftPersistence() {
  if (!window.sessionStorage) {
    return;
  }

  const ensureCollectionRows = (form, state) => {
    form.querySelectorAll("[data-collection]").forEach((collection) => {
      const addButton = form.querySelector(
        `[data-add-row="${collection.dataset.collection}"]`
      );
      const fieldNames = Array.from(
        collection.querySelectorAll(".collection-row:first-child input[name], .collection-row:first-child select[name], .collection-row:first-child textarea[name]")
      )
        .filter((field) => field.type !== "hidden" && field.type !== "file")
        .map((field) => field.name);
      const desiredRows = Math.max(
        1,
        ...fieldNames.map((name) => (state.fields[name] || []).length || 0)
      );
      const currentRows = collection.querySelectorAll(".collection-row").length;
      if (!addButton) {
        return;
      }
      for (let index = currentRows; index < desiredRows; index += 1) {
        addButton.click();
      }
    });
  };

  const restoreFormDraft = (form) => {
    const rawState = window.sessionStorage.getItem(getFormDraftKey(form));
    if (!rawState) {
      return;
    }
    const state = JSON.parse(rawState);
    ensureCollectionRows(form, state);

    const counters = {};
    getVisibleDraftFields(form).forEach((field) => {
      const key = getDraftFieldKey(field);
      if (!key || !(key in state.fields)) {
        return;
      }
      const index = counters[key] || 0;
      const values = state.fields[key];
      const nextValue = values[index];
      counters[key] = index + 1;
      if (nextValue === undefined) {
        return;
      }
      if (field.type === "checkbox") {
        field.checked = nextValue === "1";
        field.dispatchEvent(new Event("change", { bubbles: true }));
        return;
      }
      field.value = nextValue;
      if (field.classList?.contains("date-mask-field")) {
        field.dispatchEvent(new Event("input", { bubbles: true }));
        field.dispatchEvent(new Event("blur", { bubbles: true }));
        return;
      }
      field.dispatchEvent(new Event("input", { bubbles: true }));
      field.dispatchEvent(new Event("change", { bubbles: true }));
    });

    clearFormDraft(form);
  };

  document.querySelectorAll("form").forEach((form) => {
    restoreFormDraft(form);
    form.addEventListener("submit", () => clearFormDraft(form));
  });
}

function setupPendingSelections() {
  if (!window.sessionStorage) {
    return;
  }

  const url = new URL(window.location.href);
  let shouldCleanUrl = false;

  document.querySelectorAll("form").forEach((form) => {
    const pendingKey = getPendingSelectionKey(form);
    const rawState = window.sessionStorage.getItem(pendingKey);
    if (!rawState) {
      return;
    }

    let state = null;
    try {
      state = JSON.parse(rawState);
    } catch (_error) {
      window.sessionStorage.removeItem(pendingKey);
      return;
    }

    const nextValue = url.searchParams.get(state.queryKey);
    if (!nextValue) {
      return;
    }

    let candidates = [];
    if (state.isProductPicker) {
      candidates = Array.from(form.querySelectorAll("select.product-picker"));
    } else if (state.id) {
      const byId = form.querySelector(`select[id="${state.id}"]`);
      if (byId) {
        candidates = [byId];
      }
    }
    if (!candidates.length && state.name) {
      candidates = Array.from(
        form.querySelectorAll(`select[name="${state.name}"]`)
      );
    }

    const targetSelect =
      candidates[state.index] ||
      candidates.find((select) => !select.value) ||
      candidates[0];

    if (!targetSelect) {
      window.sessionStorage.removeItem(pendingKey);
      return;
    }

    const targetOption = Array.from(targetSelect.options).find(
      (option) => option.value === nextValue
    );
    if (!targetOption) {
      window.sessionStorage.removeItem(pendingKey);
      return;
    }

    targetSelect.value = nextValue;
    targetSelect.dispatchEvent(new Event("change", { bubbles: true }));
    window.sessionStorage.removeItem(pendingKey);
    url.searchParams.delete(state.queryKey);
    shouldCleanUrl = true;
  });

  if (shouldCleanUrl) {
    window.history.replaceState({}, document.title, url.pathname + (url.search ? url.search : "") + url.hash);
  }
}

function setupScrollRestore() {
  if (!window.sessionStorage) {
    document.documentElement.removeAttribute("data-restoring-scroll");
    return;
  }

  const restoreRaw = window.sessionStorage.getItem(SCROLL_RESTORE_KEY);
  if (restoreRaw) {
    try {
      const restoreState = JSON.parse(restoreRaw);
      const currentPath = `${window.location.pathname}${window.location.search}`;
      if (restoreState.path === currentPath || restoreState.path === window.location.pathname) {
        window.requestAnimationFrame(() => {
          if (restoreState.targetId) {
            const target = document.getElementById(restoreState.targetId);
            target?.scrollIntoView({ block: "start" });
          } else {
            window.scrollTo(0, Number(restoreState.scrollY) || 0);
          }
          window.requestAnimationFrame(() => {
            document.documentElement.removeAttribute("data-restoring-scroll");
          });
        });
      } else {
        document.documentElement.removeAttribute("data-restoring-scroll");
      }
    } catch (_error) {
      document.documentElement.removeAttribute("data-restoring-scroll");
    }
    window.sessionStorage.removeItem(SCROLL_RESTORE_KEY);
  } else {
    document.documentElement.removeAttribute("data-restoring-scroll");
  }

  document.querySelectorAll(".sort-link, .preserve-scroll-link").forEach((link) => {
    if (link.dataset.scrollRestoreReady === "1") {
      return;
    }
    if (!link.getAttribute("href")) {
      return;
    }
    link.dataset.scrollRestoreReady = "1";
    link.addEventListener("click", () => {
      const nextUrl = new URL(link.href, window.location.origin);
      window.sessionStorage.setItem(
        SCROLL_RESTORE_KEY,
        JSON.stringify({
          path: `${nextUrl.pathname}${nextUrl.search}`,
          scrollY: window.scrollY || window.pageYOffset || 0,
          targetId: "materials-catalog-panel",
        })
      );
    });
  });
}

function setupMaterialsCatalogClientFiltering() {
  const table = document.getElementById("materials-catalog-table");
  const body = document.getElementById("materials-catalog-body");
  if (!table || !body) {
    return;
  }

  const panel = document.getElementById(MATERIALS_CATALOG_PANEL_ID);
  const hiddenSortField = document.querySelector('#materials-filter-form input[name="sort"]');
  const hiddenDirectionField = document.querySelector(
    '#materials-filter-form input[name="direction"]'
  );
  const clearButton = document.querySelector(".table-clear-link");
  const sortButtons = Array.from(table.querySelectorAll(".sort-link[data-sort-key]"));
  const popovers = Array.from(table.querySelectorAll(".header-filter-popover"));
  const filterControls = MATERIALS_FILTER_FIELDS.reduce((accumulator, fieldName) => {
    const input = table.querySelector(`.header-filter-control[name="${fieldName}"]`);
    if (input) {
      accumulator[fieldName] = input;
    }
    return accumulator;
  }, {});
  const rows = Array.from(body.querySelectorAll("tr[data-material-row]")).map((row, index) => ({
    row,
    index,
    values: {
      sku: row.dataset.sku || "",
      material_type: row.dataset.materialType || "",
      color: row.dataset.color || "",
      name: row.dataset.name || "",
      manufacturer_name: row.dataset.manufacturerName || "",
      lot_number: row.dataset.lotNumber || "",
      location: row.dataset.location || "",
    },
  }));

  const state = {
    sortKey: table.dataset.initialSortKey || "name",
    sortDirection: table.dataset.initialSortDirection === "desc" ? "desc" : "asc",
    filters: MATERIALS_FILTER_FIELDS.reduce((accumulator, fieldName) => {
      accumulator[fieldName] = String(filterControls[fieldName]?.value || "").trim();
      return accumulator;
    }, {}),
  };

  const closeAllPopovers = () => {
    popovers.forEach((popover) => popover.classList.remove("is-open"));
  };

  const alignCatalogPanelToViewport = () => {
    if (!panel) {
      return;
    }
    const panelTop = panel.getBoundingClientRect().top + window.scrollY - 12;
    window.scrollTo({
      top: Math.max(panelTop, 0),
      behavior: "auto",
    });
  };

  const syncUrlState = () => {
    const url = new URL(window.location.href);
    MATERIALS_FILTER_FIELDS.forEach((fieldName) => {
      const value = String(state.filters[fieldName] || "").trim();
      if (value) {
        url.searchParams.set(fieldName, value);
      } else {
        url.searchParams.delete(fieldName);
      }
    });
    if (state.sortKey) {
      url.searchParams.set("sort", state.sortKey);
    } else {
      url.searchParams.delete("sort");
    }
    if (state.sortDirection && state.sortDirection !== "asc") {
      url.searchParams.set("direction", state.sortDirection);
    } else {
      url.searchParams.delete("direction");
    }
    url.hash = `#${MATERIALS_CATALOG_PANEL_ID}`;
    window.history.replaceState({}, document.title, `${url.pathname}${url.search}${url.hash}`);
  };

  const getRowValue = (rowData, fieldName) => String(rowData.values[fieldName] || "");

  const rowMatchesFilters = (rowData, filtersToApply) =>
    MATERIALS_FILTER_FIELDS.every((fieldName) => {
      const filterValue = String(filtersToApply[fieldName] || "").trim();
      if (!filterValue) {
        return true;
      }
      return normalizeText(getRowValue(rowData, fieldName)).includes(normalizeText(filterValue));
    });

  const compareRows = (leftRow, rightRow) => {
    const leftValue = getRowValue(leftRow, state.sortKey);
    const rightValue = getRowValue(rightRow, state.sortKey);
    const order = leftValue.localeCompare(rightValue, "pt-BR", {
      numeric: true,
      sensitivity: "base",
    });
    if (order !== 0) {
      return state.sortDirection === "asc" ? order : -order;
    }
    return leftRow.index - rightRow.index;
  };

  const updateSortIndicators = () => {
    sortButtons.forEach((button) => {
      const isActive = button.dataset.sortKey === state.sortKey;
      button.classList.toggle("is-active", isActive);
      button.textContent = !isActive ? "↕" : state.sortDirection === "asc" ? "↓" : "↑";
    });
    if (hiddenSortField) {
      hiddenSortField.value = state.sortKey;
    }
    if (hiddenDirectionField) {
      hiddenDirectionField.value = state.sortDirection;
    }
  };

  const bindPopoverOptionEvents = (button, fieldName, input, popover) => {
    button.addEventListener("mouseenter", () => {
      popover
        .querySelectorAll(".header-filter-option")
        .forEach((option) => option.classList.remove("is-active"));
      button.classList.add("is-active");
    });
    button.addEventListener("mousedown", (event) => {
      event.preventDefault();
      const nextValue = button.dataset.filterValue || "";
      input.value = nextValue;
      state.filters[fieldName] = nextValue;
      closeAllPopovers();
      renderCatalog();
      syncUrlState();
      alignCatalogPanelToViewport();
    });
  };

  const renderPopoverOptions = (fieldName) => {
    const popover = table.querySelector(
      `.header-filter-popover input[name="${fieldName}"]`
    )?.closest(".header-filter-popover");
    const input = filterControls[fieldName];
    if (!popover || !input) {
      return;
    }
    const optionsWrap = popover.querySelector(".header-filter-options");
    if (!optionsWrap) {
      return;
    }

    const filtersWithoutCurrent = { ...state.filters, [fieldName]: "" };
    const optionValues = rows
      .filter((rowData) => rowMatchesFilters(rowData, filtersWithoutCurrent))
      .map((rowData) => getRowValue(rowData, fieldName).trim())
      .filter(Boolean)
      .filter((value, index, collection) => collection.indexOf(value) === index)
      .sort((leftValue, rightValue) =>
        leftValue.localeCompare(rightValue, "pt-BR", { numeric: true, sensitivity: "base" })
      );

    const term = normalizeText(input.value);
    const visibleValues = optionValues.filter((value) => normalizeText(value).includes(term));

    optionsWrap.innerHTML = "";
    visibleValues.forEach((value, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "header-filter-option";
      if (index === 0) {
        button.classList.add("is-active");
      }
      button.dataset.filterName = fieldName;
      button.dataset.filterValue = value;
      button.textContent = value;
      bindPopoverOptionEvents(button, fieldName, input, popover);
      optionsWrap.appendChild(button);
    });
  };

  const renderAllPopoverOptions = () => {
    MATERIALS_FILTER_FIELDS.forEach((fieldName) => renderPopoverOptions(fieldName));
  };

  const renderCatalog = () => {
    const filteredRows = rows.filter((rowData) => rowMatchesFilters(rowData, state.filters));
    filteredRows.sort(compareRows);
    body.innerHTML = "";
    filteredRows.forEach((rowData) => body.appendChild(rowData.row));
    updateSortIndicators();
    renderAllPopoverOptions();
  };

  table.querySelectorAll(".header-filter-trigger").forEach((trigger) => {
    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const target = document.getElementById(trigger.dataset.filterTarget || "");
      if (!target) {
        return;
      }
      const willOpen = !target.classList.contains("is-open");
      closeAllPopovers();
      if (willOpen) {
        target.classList.add("is-open");
        const input = target.querySelector(".header-filter-control");
        if (input?.name) {
          renderPopoverOptions(input.name);
        }
        input?.focus();
        input?.select?.();
      }
    });
  });

  Object.entries(filterControls).forEach(([fieldName, input]) => {
    const popover = input.closest(".header-filter-popover");
    input.addEventListener("focus", () => renderPopoverOptions(fieldName));
    input.addEventListener("input", () => {
      state.filters[fieldName] = input.value.trim();
      renderCatalog();
      renderPopoverOptions(fieldName);
      syncUrlState();
      popover?.classList.add("is-open");
      alignCatalogPanelToViewport();
    });
    input.addEventListener("keydown", (event) => {
      const visibleOptions = Array.from(
        popover?.querySelectorAll(".header-filter-option") || []
      ).filter((option) => !option.hidden);
      let activeIndex = visibleOptions.findIndex((option) =>
        option.classList.contains("is-active")
      );

      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (!visibleOptions.length) {
          return;
        }
        activeIndex = Math.min(activeIndex + 1, visibleOptions.length - 1);
        visibleOptions.forEach((option) => option.classList.remove("is-active"));
        visibleOptions[activeIndex].classList.add("is-active");
        visibleOptions[activeIndex].scrollIntoView({ block: "nearest" });
      }

      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (!visibleOptions.length) {
          return;
        }
        activeIndex = activeIndex <= 0 ? 0 : activeIndex - 1;
        visibleOptions.forEach((option) => option.classList.remove("is-active"));
        visibleOptions[activeIndex].classList.add("is-active");
        visibleOptions[activeIndex].scrollIntoView({ block: "nearest" });
      }

      if (event.key === "Enter" || event.key === "Tab") {
        if (visibleOptions.length) {
          event.preventDefault();
          const chosen = visibleOptions[Math.max(activeIndex, 0)] || visibleOptions[0];
          const nextValue = chosen.dataset.filterValue || "";
          input.value = nextValue;
          state.filters[fieldName] = nextValue;
          closeAllPopovers();
          renderCatalog();
          syncUrlState();
          alignCatalogPanelToViewport();
          if (event.key === "Tab") {
            const focusable = Array.from(
              document.querySelectorAll(
                "button:not([disabled]), input:not([type='hidden']):not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href]"
              )
            ).filter((element) => element.offsetParent !== null);
            const currentIndex = focusable.indexOf(input);
            const nextField = focusable[currentIndex + 1];
            window.setTimeout(() => nextField?.focus(), 0);
          }
          return;
        }
        closeAllPopovers();
      }

      if (event.key === "Escape") {
        closeAllPopovers();
      }
    });
  });

  sortButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const sortKey = button.dataset.sortKey || "name";
      if (state.sortKey === sortKey) {
        state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = sortKey;
        state.sortDirection = "asc";
      }
      renderCatalog();
      syncUrlState();
      alignCatalogPanelToViewport();
    });
  });

  clearButton?.addEventListener("click", (event) => {
    event.preventDefault();
    MATERIALS_FILTER_FIELDS.forEach((fieldName) => {
      state.filters[fieldName] = "";
      if (filterControls[fieldName]) {
        filterControls[fieldName].value = "";
      }
    });
    state.sortKey = "name";
    state.sortDirection = "asc";
    closeAllPopovers();
    renderCatalog();
    syncUrlState();
    alignCatalogPanelToViewport();
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest(".table-header-filter")) {
      return;
    }
    closeAllPopovers();
  });

  renderCatalog();
}

function setupAutoFilterForms() {
  if (document.getElementById("materials-catalog-table")) {
    return;
  }
  const forms = Array.from(document.querySelectorAll("form.table-filter-form"));
  if (!forms.length) {
    return;
  }

  const navigateToCatalogAnchor = (url) => {
    url.hash = MATERIALS_CATALOG_PANEL_ID;
    window.location.href = url.toString();
  };

  const submitWithScrollRestore = (form) => {
    const action = form.getAttribute("action") || window.location.pathname;
    const url = new URL(action, window.location.origin);
    const formData = new FormData(form);
    for (const [key, value] of formData.entries()) {
      if (String(value).trim()) {
        url.searchParams.set(key, String(value));
      } else {
        url.searchParams.delete(key);
      }
    }

    if (window.sessionStorage) {
      window.sessionStorage.setItem(
        SCROLL_RESTORE_KEY,
        JSON.stringify({
          path: `${url.pathname}${url.search}`,
          scrollY: window.scrollY || window.pageYOffset || 0,
          targetId: MATERIALS_CATALOG_PANEL_ID,
        })
      );
    }
    navigateToCatalogAnchor(url);
  };

  forms.forEach((form) => {
    const controls = Array.from(
      document.querySelectorAll(
        `#${form.id} input[name]:not([type='hidden']), #${form.id} select[name], #${form.id} textarea[name], input[form="${form.id}"][name]:not([type='hidden']), select[form="${form.id}"][name], textarea[form="${form.id}"][name]`
      )
    );
    controls.forEach((control) => {
      if (control.dataset.autoFilterReady === "1") {
        return;
      }
      control.dataset.autoFilterReady = "1";
      if (control.closest(".header-filter-popover")) {
        return;
      }

      let debounceTimer = null;
      control.addEventListener("input", () => {
        window.clearTimeout(debounceTimer);
        debounceTimer = window.setTimeout(() => {
          submitWithScrollRestore(form);
        }, 260);
      });
      control.addEventListener("change", () => {
        window.clearTimeout(debounceTimer);
        submitWithScrollRestore(form);
      });
    });
  });
}

function setupHeaderFilterPopovers() {
  if (document.getElementById("materials-catalog-table")) {
    return;
  }
  const triggers = Array.from(document.querySelectorAll(".header-filter-trigger"));
  if (!triggers.length) {
    return;
  }

  const closeAll = () => {
    document.querySelectorAll(".header-filter-popover.is-open").forEach((popover) => {
      popover.classList.remove("is-open");
    });
  };

  const navigateToCatalogAnchor = (url) => {
    url.hash = MATERIALS_CATALOG_PANEL_ID;
    window.location.href = url.toString();
  };

  triggers.forEach((trigger) => {
    if (trigger.dataset.popoverReady === "1") {
      return;
    }
    trigger.dataset.popoverReady = "1";
    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const target = document.getElementById(trigger.dataset.filterTarget || "");
      if (!target) {
        return;
      }
      const willOpen = !target.classList.contains("is-open");
      closeAll();
      if (willOpen) {
        target.classList.add("is-open");
        target.querySelector("input, select, textarea")?.focus();
        target.querySelector("input, select, textarea")?.select?.();
      }
    });
  });

  const applyFilterFromPopover = (input, value = null) => {
    if (!input) {
      return;
    }
    if (value !== null) {
      input.value = value;
    }
    const formId = input.getAttribute("form");
    const form = formId ? document.getElementById(formId) : input.form;
    if (!form) {
      return;
    }
    const action = form.getAttribute("action") || window.location.pathname;
    const url = new URL(action, window.location.origin);
    const formData = new FormData(form);
    for (const [key, rawValue] of formData.entries()) {
      if (String(rawValue).trim()) {
        url.searchParams.set(key, String(rawValue));
      } else {
        url.searchParams.delete(key);
      }
    }
    if (window.sessionStorage) {
      window.sessionStorage.setItem(
        SCROLL_RESTORE_KEY,
        JSON.stringify({
          path: `${url.pathname}${url.search}`,
          scrollY: window.scrollY || window.pageYOffset || 0,
          targetId: MATERIALS_CATALOG_PANEL_ID,
        })
      );
    }
    closeAll();
    navigateToCatalogAnchor(url);
  };

  document.querySelectorAll(".header-filter-popover").forEach((popover) => {
    const input = popover.querySelector(".header-filter-control");
    const options = Array.from(popover.querySelectorAll(".header-filter-option"));
    if (!input) {
      return;
    }

    let activeIndex = -1;
    const getVisibleOptions = () => options.filter((option) => !option.hidden);

    const refreshOptions = () => {
      const term = String(input.value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase();
      options.forEach((option) => {
        const haystack = String(option.dataset.filterValue || option.textContent || "")
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "")
          .toLowerCase();
        const matches = !term || haystack.includes(term);
        option.hidden = !matches;
        option.classList.remove("is-active");
      });
      const visibleOptions = getVisibleOptions();
      activeIndex = visibleOptions.length ? 0 : -1;
      if (visibleOptions[0]) {
        visibleOptions[0].classList.add("is-active");
      }
    };

    options.forEach((option) => {
      option.addEventListener("mouseenter", () => {
        const visibleOptions = getVisibleOptions();
        visibleOptions.forEach((item) => item.classList.remove("is-active"));
        option.classList.add("is-active");
        activeIndex = visibleOptions.indexOf(option);
      });
      option.addEventListener("mousedown", (event) => {
        event.preventDefault();
        applyFilterFromPopover(input, option.dataset.filterValue || option.textContent.trim());
      });
      option.addEventListener("click", () => {
        applyFilterFromPopover(input, option.dataset.filterValue || option.textContent.trim());
      });
    });

    input.addEventListener("focus", refreshOptions);
    input.addEventListener("input", refreshOptions);
    input.addEventListener("keydown", (event) => {
      const visibleOptions = getVisibleOptions();
      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (!visibleOptions.length) {
          return;
        }
        activeIndex = Math.min(activeIndex + 1, visibleOptions.length - 1);
        visibleOptions.forEach((item) => item.classList.remove("is-active"));
        visibleOptions[activeIndex].classList.add("is-active");
        visibleOptions[activeIndex].scrollIntoView({ block: "nearest" });
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (!visibleOptions.length) {
          return;
        }
        activeIndex = activeIndex <= 0 ? 0 : activeIndex - 1;
        visibleOptions.forEach((item) => item.classList.remove("is-active"));
        visibleOptions[activeIndex].classList.add("is-active");
        visibleOptions[activeIndex].scrollIntoView({ block: "nearest" });
      }
      if (event.key === "Enter" || event.key === "Tab") {
        if (visibleOptions.length) {
          event.preventDefault();
          const chosen = visibleOptions[Math.max(activeIndex, 0)] || visibleOptions[0];
          applyFilterFromPopover(input, chosen.dataset.filterValue || chosen.textContent.trim());
          return;
        }
        if (input.value.trim()) {
          event.preventDefault();
          applyFilterFromPopover(input, input.value.trim());
        }
      }
      if (event.key === "Escape") {
        closeAll();
      }
    });
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest(".table-header-filter")) {
      return;
    }
    closeAll();
  });
}

function setupMaterialsCatalogAnchor() {
  if (window.location.pathname !== "/materials" || document.getElementById("materials-catalog-table")) {
    return;
  }

  const url = new URL(window.location.href);
  const hasFilterParams = [
    "sku",
    "material_type",
    "color",
    "name",
    "manufacturer_name",
    "lot_number",
    "location",
  ].some((key) => url.searchParams.get(key));

  const shouldAnchorToCatalog =
    url.hash === `#${MATERIALS_CATALOG_PANEL_ID}` || hasFilterParams;

  if (!shouldAnchorToCatalog) {
    return;
  }

  const moveToCatalog = () => {
    const panel = document.getElementById(MATERIALS_CATALOG_PANEL_ID);
    if (!panel) {
      return;
    }
    panel.scrollIntoView({ block: "start" });
  };

  window.requestAnimationFrame(() => {
    moveToCatalog();
    window.requestAnimationFrame(moveToCatalog);
  });
}

function setupSearchableSelects(root = document) {
  const selects = Array.from(root.querySelectorAll("select"));
  const normalize = (value) =>
    String(value || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase();
  const focusNextField = (field) => {
    const focusableFields = Array.from(
      field.form?.querySelectorAll(
        "input:not([type='hidden']):not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled])"
      ) || []
    ).filter((item) => item.offsetParent !== null);
    const currentIndex = focusableFields.indexOf(field);
    const nextField = focusableFields[currentIndex + 1];
    if (nextField) {
      window.setTimeout(() => nextField.focus(), 0);
    }
  };

  selects.forEach((select) => {
    if (select.dataset.searchableReady === "1") {
      return;
    }
    if (select.dataset.nativeSelect === "1") {
      return;
    }
    select.dataset.searchableReady = "1";
    select._searchableOptions = Array.from(select.options);

    const wrapper = document.createElement("div");
    wrapper.className = "searchable-select-wrapper";
    const searchInput = document.createElement("input");
    searchInput.type = "text";
    searchInput.className = "select-search-input";
    searchInput.placeholder = "Pesquisar...";
    searchInput.autocomplete = "off";
    const resultsList = document.createElement("div");
    resultsList.className = "select-search-results";
    resultsList.setAttribute("role", "listbox");

    const getSelectedText = () => {
      const selectedOption = select.selectedOptions[0];
      return selectedOption && selectedOption.value
        ? selectedOption.textContent.trim()
        : "";
    };

    select.parentNode.insertBefore(wrapper, select);
    wrapper.appendChild(searchInput);
    wrapper.appendChild(select);
    wrapper.appendChild(resultsList);
    select.classList.add("native-select-hidden");
    searchInput.value = getSelectedText();
    let activeValue = select.value || "";

    const closeFilteredList = () => {
      wrapper.classList.remove("is-open");
    };

    const setActiveOption = (value) => {
      activeValue = value || "";
      Array.from(resultsList.querySelectorAll(".select-search-option")).forEach((item) => {
        item.classList.toggle("is-active", item.dataset.value === activeValue);
      });
      resultsList.querySelector(".select-search-option.is-active")?.scrollIntoView({
        block: "nearest",
      });
    };

    const chooseOption = (option) => {
      if (option.disabled) {
        return;
      }
      select.value = option.value;
      searchInput.value = option.value ? option.textContent.trim() : "";
      activeValue = option.value;
      select.dispatchEvent(new Event("change", { bubbles: true }));
      closeFilteredList();
    };

    select.addEventListener("change", () => {
      const selectedOption = select.selectedOptions[0];
      searchInput.value =
        selectedOption && selectedOption.value
          ? selectedOption.textContent.trim()
          : "";
      activeValue = select.value || "";
      setActiveOption(activeValue);
    });

    const openFilteredList = (showAllOptions = false) => {
      const searchTerm = showAllOptions ? "" : normalize(searchInput.value);
      resultsList.innerHTML = "";
      const filteredOptions = select._searchableOptions.filter((option) => {
        const searchableText = normalize(`${option.textContent} ${option.value}`);
        return !searchTerm || searchableText.includes(searchTerm);
      });
      const optionsToShow = filteredOptions.length ? filteredOptions : [];

      if (!optionsToShow.length) {
        const emptyItem = document.createElement("button");
        emptyItem.type = "button";
        emptyItem.className = "select-search-option is-empty";
        emptyItem.textContent = "Nenhum resultado encontrado";
        emptyItem.disabled = true;
        resultsList.appendChild(emptyItem);
      }

      optionsToShow.slice(0, 80).forEach((option) => {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "select-search-option";
        item.textContent = option.textContent.trim();
        item.dataset.value = option.value;
        item.setAttribute("role", "option");
        if (option.value === select.value) {
          item.classList.add("is-selected");
        }
        item.addEventListener("mouseenter", () => setActiveOption(option.value));
        item.addEventListener("mousedown", (event) => {
          event.preventDefault();
          chooseOption(option);
        });
        item.addEventListener("click", () => chooseOption(option));
        resultsList.appendChild(item);
      });
      wrapper.classList.add("is-open");
      const currentSelected =
        optionsToShow.find((option) => option.value === activeValue) ||
        optionsToShow.find((option) => option.value === select.value) ||
        optionsToShow.find((option) => !option.disabled) ||
        null;
      setActiveOption(currentSelected?.value || "");
    };

    const delayedClose = () => {
      window.setTimeout(() => {
        if (wrapper.contains(document.activeElement)) {
          return;
        }
        closeFilteredList();
      }, 120);
    };

    searchInput.addEventListener("input", () => openFilteredList());
    searchInput.addEventListener("focus", () => {
      searchInput.select();
    });
    searchInput.addEventListener("click", () => {
      searchInput.select();
      openFilteredList(true);
    });
    searchInput.addEventListener("blur", delayedClose);
    searchInput.addEventListener("keydown", (event) => {
      const visibleOptions = Array.from(
        resultsList.querySelectorAll(".select-search-option:not(.is-empty)")
      );
      const currentIndex = visibleOptions.findIndex(
        (item) => item.dataset.value === activeValue
      );
      if (event.key === "Enter") {
        const chosenOption =
          visibleOptions[currentIndex >= 0 ? currentIndex : 0];
        if (chosenOption) {
          event.preventDefault();
          const option = select._searchableOptions.find(
            (item) => item.value === chosenOption.dataset.value
          );
          if (option) {
            chooseOption(option);
          }
        }
      }
      if (event.key === "Tab" && wrapper.classList.contains("is-open")) {
        const chosenOption =
          visibleOptions[currentIndex >= 0 ? currentIndex : 0];
        if (chosenOption) {
          event.preventDefault();
          const option = select._searchableOptions.find(
            (item) => item.value === chosenOption.dataset.value
          );
          if (option) {
            chooseOption(option);
            focusNextField(searchInput);
          }
        }
      }
      if (event.key === "Escape") {
        closeFilteredList();
      }
      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (!wrapper.classList.contains("is-open")) {
          openFilteredList(true);
          return;
        }
        const nextIndex =
          currentIndex < 0 ? 0 : Math.min(currentIndex + 1, visibleOptions.length - 1);
        if (visibleOptions[nextIndex]) {
          setActiveOption(visibleOptions[nextIndex].dataset.value);
        }
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (!wrapper.classList.contains("is-open")) {
          openFilteredList(true);
          return;
        }
        const previousIndex =
          currentIndex < 0 ? visibleOptions.length - 1 : Math.max(currentIndex - 1, 0);
        if (visibleOptions[previousIndex]) {
          setActiveOption(visibleOptions[previousIndex].dataset.value);
        }
      }
    });
    select.addEventListener("change", () => {
      searchInput.value = getSelectedText();
      activeValue = select.value || "";
    });
  });
}

function setupCurrencyFields() {
  const fields = Array.from(document.querySelectorAll(".currency-field"));
  const parseCurrencyValue = (rawValue) => {
    const normalized = String(rawValue || "")
      .replace("R$", "")
      .replace(/\s/g, "")
      .trim();
    if (!normalized) {
      return 0;
    }
    if (normalized.includes(",")) {
      return Number(normalized.replace(/\./g, "").replace(",", ".")) || 0;
    }
    if (normalized.includes(".")) {
      const pieces = normalized.split(".");
      if (pieces.length === 2 && pieces[1].length <= 2) {
        return Number(normalized) || 0;
      }
      return Number(normalized.replace(/\./g, "")) || 0;
    }
    return Number(normalized) || 0;
  };

  const formatCurrency = (rawValue) => {
    const value = parseCurrencyValue(rawValue);
    return new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  fields.forEach((field) => {
    field.value = formatCurrency(field.value);
    field.addEventListener("blur", () => {
      field.value = formatCurrency(field.value);
    });
    field.addEventListener("change", () => {
      field.value = formatCurrency(field.value);
    });
    field.form?.addEventListener("submit", () => {
      field.value = formatCurrency(field.value);
    });
  });
}

function setupDateMasks() {
  const fields = Array.from(document.querySelectorAll(".date-mask-field"));

  const isoToBrazilian = (value) => {
    const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!match) {
      return value || "";
    }
    return `${match[3]}/${match[2]}/${match[1].slice(2)}`;
  };

  const formatDateInput = (value) => {
    const digits = String(value || "").replace(/\D/g, "").slice(0, 6);
    if (digits.length <= 2) {
      return digits;
    }
    if (digits.length <= 4) {
      return `${digits.slice(0, 2)}/${digits.slice(2)}`;
    }
    return `${digits.slice(0, 2)}/${digits.slice(2, 4)}/${digits.slice(4)}`;
  };

  const normalizeYear = (value) => {
    const digits = String(value || "").replace(/\D/g, "");
    return digits.length === 2 ? `20${digits}` : digits;
  };

  const brazilianToIso = (value) => {
    const digits = String(value || "").replace(/\D/g, "");
    if (digits.length !== 6) {
      return "";
    }
    const day = digits.slice(0, 2);
    const month = digits.slice(2, 4);
    const normalizedYear = normalizeYear(digits.slice(4, 6));
    const date = new Date(`${normalizedYear}-${month}-${day}T00:00:00`);
    if (
      Number.isNaN(date.getTime()) ||
      date.getFullYear() !== Number(normalizedYear) ||
      date.getMonth() + 1 !== Number(month) ||
      date.getDate() !== Number(day)
    ) {
      return "";
    }
    return `${normalizedYear}-${month}-${day}`;
  };

  const focusNextField = (field) => {
    const focusableFields = Array.from(
      field.form?.querySelectorAll(
        "input:not([type='hidden']):not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled])"
      ) || []
    ).filter((item) => item.offsetParent !== null);
    const currentIndex = focusableFields.indexOf(field);
    const nextField = focusableFields[currentIndex + 1];
    if (nextField) {
      window.setTimeout(() => nextField.focus(), 0);
    }
  };

  fields.forEach((field) => {
    const hiddenField = field.form?.querySelector(
      `input[type="hidden"][name="${field.dataset.dateField}"]`
    );
    if (!hiddenField) {
      return;
    }

    field.maxLength = 8;
    field.placeholder = "dd/mm/aa";
    field.value = isoToBrazilian(field.value || hiddenField.value);

    const updateHiddenField = () => {
      hiddenField.value = brazilianToIso(field.value);
    };

    field.addEventListener("input", () => {
      const digits = field.value.replace(/\D/g, "").slice(0, 6);
      field.value = formatDateInput(field.value);
      updateHiddenField();
      if (digits.length === 6 && hiddenField.value) {
        focusNextField(field);
      }
    });
    field.addEventListener("blur", updateHiddenField);
    updateHiddenField();
  });
}

function setupCommercialDateDefaults() {
  const forms = Array.from(document.querySelectorAll(".job-form"));

  const isoToBrazilian = (value) => {
    const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!match) {
      return "";
    }
    return `${match[3]}/${match[2]}/${match[1].slice(2)}`;
  };

  const addDays = (isoDate, days) => {
    const match = String(isoDate || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) {
      return "";
    }
    const date = new Date(`${isoDate}T00:00:00`);
    if (Number.isNaN(date.getTime())) {
      return "";
    }
    date.setDate(date.getDate() + days);
    return date.toISOString().slice(0, 10);
  };

  forms.forEach((form) => {
    const createdAtHidden = form.querySelector("input[type='hidden'][name='created_at']");
    const validUntilHidden = form.querySelector("input[type='hidden'][name='valid_until']");
    const createdAtField = form.querySelector(".date-mask-field[data-date-field='created_at']");
    const validUntilField = form.querySelector(".date-mask-field[data-date-field='valid_until']");
    if (!createdAtHidden || !validUntilHidden || !createdAtField || !validUntilField) {
      return;
    }

    let validUntilWasEdited =
      Boolean(validUntilHidden.value) &&
      validUntilHidden.value !== addDays(createdAtHidden.value, 5);
    validUntilField.addEventListener("input", () => {
      validUntilWasEdited = true;
    });

    createdAtField.addEventListener("input", () => {
      if (validUntilWasEdited) {
        return;
      }
      const defaultValidUntil = addDays(createdAtHidden.value, 5);
      if (!defaultValidUntil) {
        return;
      }
      validUntilHidden.value = defaultValidUntil;
      validUntilField.value = isoToBrazilian(defaultValidUntil);
    });
  });
}

function setupPrinterHourlyCost() {
  const purchaseValueField =
    document.querySelector("[name='purchase_value']") ||
    document.querySelector("[name='price']");
  const usefulLifeField = document.querySelector("[name='useful_life_hours']");
  const energyWattsField =
    document.querySelector("[name='energy_watts']") ||
    document.querySelector("[name='power_watts']");
  const kwhCostField = document.querySelector("[name='kwh_cost']");
  const depreciationHourlyCostField = document.querySelector(
    "[name='depreciation_hourly_cost']"
  );
  const energyHourlyCostField = document.querySelector("[name='energy_hourly_cost']");
  const hourlyCostField = document.querySelector("[name='hourly_cost']");

  if (
    !purchaseValueField ||
    !usefulLifeField ||
    !energyWattsField ||
    !kwhCostField ||
    !hourlyCostField
  ) {
    return;
  }

  const parseBrazilianDecimal = (rawValue) => {
    const normalized = rawValue
      .replace("R$", "")
      .replace(/\s/g, "")
      .replace(/\./g, "")
      .replace(",", ".");
    return Number(normalized) || 0;
  };

  const formatBrazilianDecimal = (value) =>
    new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);

  const updateHourlyCost = () => {
    const purchaseValue = parseBrazilianDecimal(purchaseValueField.value);
    const usefulLifeHours = Number(usefulLifeField.value) || 0;
    const energyWatts = Number(energyWattsField.value) || 0;
    const kwhCost = parseBrazilianDecimal(kwhCostField.value);
    const depreciationCost = usefulLifeHours > 0 ? purchaseValue / usefulLifeHours : 0;
    const energyCost = (energyWatts / 1000) * kwhCost;
    const hourlyCost = depreciationCost + energyCost;
    if (depreciationHourlyCostField) {
      depreciationHourlyCostField.value = formatBrazilianDecimal(depreciationCost);
    }
    if (energyHourlyCostField) {
      energyHourlyCostField.value = formatBrazilianDecimal(energyCost);
    }
    hourlyCostField.value = formatBrazilianDecimal(hourlyCost);
  };

  purchaseValueField.addEventListener("input", updateHourlyCost);
  usefulLifeField.addEventListener("input", updateHourlyCost);
  energyWattsField.addEventListener("input", updateHourlyCost);
  kwhCostField.addEventListener("input", updateHourlyCost);
  updateHourlyCost();
}

function setupJobPrinterCosts() {
  const forms = Array.from(document.querySelectorAll(".job-form"));
  forms.forEach((form) => {
    const printerSelect = form.querySelector("[name='printer_id']");
    const energyField = form.querySelector("[name='energy_cost_per_hour']");
    const operatingField = form.querySelector("[name='operating_cost_per_hour']");
    if (!printerSelect || !energyField || !operatingField) {
      return;
    }

    const formatDecimal = (value) =>
      String(Math.max(Number(value) || 0, 0).toFixed(2));

    printerSelect.addEventListener("change", () => {
      const selectedOption = printerSelect.selectedOptions[0];
      if (!selectedOption || !selectedOption.value) {
        return;
      }

      energyField.value = formatDecimal(selectedOption.dataset.energyCost);
      operatingField.value = formatDecimal(selectedOption.dataset.operatingCost);
    });
  });
}

function setupJobDryerCosts() {
  const forms = Array.from(document.querySelectorAll(".job-form"));
  forms.forEach((form) => {
    const dryerSelect = form.querySelector("[name='filament_dryer_id']");
    const dryerCostField = form.querySelector("[name='dryer_cost_per_hour']");
    if (!dryerSelect || !dryerCostField) {
      return;
    }

    dryerSelect.addEventListener("change", () => {
      const selectedOption = dryerSelect.selectedOptions[0];
      if (!selectedOption || !selectedOption.value) {
        dryerCostField.value = "0.00";
        return;
      }

      dryerCostField.value = String(
        Math.max(Number(selectedOption.dataset.hourlyCost) || 0, 0).toFixed(2)
      );
    });
  });
}

function setupJobInlineTotals() {
  const forms = Array.from(document.querySelectorAll(".job-form"));
  const parseNumber = (value) => Number(String(value || "0").replace(",", ".")) || 0;
  const currency = (value) =>
    new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
    }).format(value);

  forms.forEach((form) => {
    const getServiceTotal = (row) => {
      const quantity = parseNumber(row.querySelector(".order-quantity-field")?.value) || 1;
      const unitPrice = parseNumber(row.querySelector("[name='service_unit_price']")?.value);
      const additions = parseNumber(row.querySelector(".service-additions")?.value);
      const discounts = parseNumber(row.querySelector(".service-discounts")?.value);
      return (quantity * unitPrice) + additions - discounts;
    };

    const updateServiceTotals = () => {
      form.querySelectorAll("[data-collection='services'] .collection-row").forEach(
        (row) => {
          const quantity = parseNumber(row.querySelector(".order-quantity-field")?.value) || 1;
          const itemNameField = row.querySelector("[name='item_name']");
          const serviceQuantitySync = row.querySelector(".service-quantity-sync");
          const serviceNameSync = row.querySelector(".service-name-sync");
          const serviceCategoryField = row.querySelector(".service-category-display");
          const serviceCategorySync = row.querySelector(".service-category-sync");
          const serviceNotesSync = row.querySelector(".service-notes-sync");
          const additions = parseNumber(row.querySelector(".service-additions")?.value);
          const discounts = parseNumber(row.querySelector(".service-discounts")?.value);
          const serviceTotalField = row.querySelector(".service-line-total");

          if (serviceQuantitySync) {
            serviceQuantitySync.value = String(quantity);
          }
          if (serviceNameSync && itemNameField) {
            serviceNameSync.value = itemNameField.value;
          }
          if (serviceCategorySync && serviceCategoryField) {
            serviceCategorySync.value = serviceCategoryField.value || "";
          }
          if (serviceNotesSync) {
            const notes = [];
            if (additions > 0) {
              notes.push(`Acrescimos: ${currency(additions)}`);
            }
            if (discounts > 0) {
              notes.push(`Descontos: ${currency(discounts)}`);
            }
            serviceNotesSync.value = notes.join(" | ");
          }
          if (serviceTotalField) {
            serviceTotalField.value = currency(getServiceTotal(row));
          }
        }
      );
    };

    const renderBreakdown = (selector, items) => {
      const container = form.querySelector(selector);
      if (!container) {
        return;
      }
      container.innerHTML = "";
      items
        .filter((item) => item.total > 0 || item.label)
        .forEach((item) => {
          const row = document.createElement("div");
          row.className = "internal-cost-subrow";

          const category = document.createElement("span");
          category.className = "internal-cost-subrow-label";
          category.textContent = `• ${item.label}`;

          const base = document.createElement("span");
          base.className = "internal-cost-subrow-base";
          base.textContent = item.base;

          const rate = document.createElement("span");
          rate.className = "internal-cost-subrow-rate";
          rate.textContent = item.rate;

          const total = document.createElement("span");
          total.className = "internal-cost-subrow-total";
          total.textContent = currency(item.total);

          row.append(category, base, rate, total);
          container.appendChild(row);
        });
    };

    const getMaterialRowCosts = (row) => {
      const materialSelect = row.querySelector("[name='material_id']");
      const printerSelect = row.querySelector("[name='printer_id']");
      const dryerSelect = row.querySelector("[name='filament_dryer_id']");
      const weightField = row.querySelector("[name='material_weight_grams']");
      const printHoursField = row.querySelector("[name='print_hours']");
      const materialOption = materialSelect?.selectedOptions?.[0];
      const printerOption = printerSelect?.selectedOptions?.[0];
      const dryerOption = dryerSelect?.selectedOptions?.[0];
      const weight = parseNumber(weightField?.value);
      const printHours = parseNumber(printHoursField?.value);
      const costPerKg = parseNumber(materialOption?.dataset.costPerKg);
      const energyRate = parseNumber(printerOption?.dataset.energyCost);
      const operatingRate = parseNumber(printerOption?.dataset.operatingCost);
      const dryerRate = parseNumber(dryerOption?.dataset.hourlyCost);
      const dryerHours = dryerSelect?.value ? printHours : 0;
      return {
        materialLabel: materialOption?.value ? materialOption.textContent.trim() : "Material",
        printerLabel: printerOption?.value ? printerOption.textContent.trim() : "Sem impressora",
        dryerLabel: dryerOption?.value ? dryerOption.textContent.trim() : "",
        weight,
        printHours,
        dryerHours,
        costPerKg,
        materialTotal: weight * (costPerKg / 1000),
        energyRate,
        energyTotal: printHours * energyRate,
        operatingRate,
        operatingTotal: printHours * operatingRate,
        dryerRate,
        dryerTotal: dryerHours * dryerRate,
      };
    };

    const getMaterialTotals = () => {
      return Array.from(
        form.querySelectorAll("[data-collection='materials'] .collection-row")
      ).reduce(
        (totals, row) => {
          const rowCosts = getMaterialRowCosts(row);
          totals.weight += rowCosts.weight;
          totals.printHours += rowCosts.printHours;
          totals.dryerHours += rowCosts.dryerHours;
          totals.cost += rowCosts.materialTotal;
          totals.energy += rowCosts.energyTotal;
          totals.operating += rowCosts.operatingTotal;
          totals.dryer += rowCosts.dryerTotal;
          totals.materialBreakdown.push({
            label: rowCosts.materialLabel,
            base: `${rowCosts.weight.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} g`,
            rate: `R$ ${rowCosts.costPerKg.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}/kg`,
            total: rowCosts.materialTotal,
          });
          totals.energyBreakdown.push({
            label: rowCosts.printerLabel,
            base: `${rowCosts.printHours.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} h`,
            rate: `R$ ${rowCosts.energyRate.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}/h`,
            total: rowCosts.energyTotal,
          });
          totals.operatingBreakdown.push({
            label: rowCosts.printerLabel,
            base: `${rowCosts.printHours.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} h`,
            rate: `R$ ${rowCosts.operatingRate.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}/h`,
            total: rowCosts.operatingTotal,
          });
          if (rowCosts.dryerLabel || rowCosts.dryerTotal > 0) {
            totals.dryerBreakdown.push({
              label: rowCosts.dryerLabel || "Sem secador",
              base: `${rowCosts.dryerHours.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} h`,
              rate: `R$ ${rowCosts.dryerRate.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}/h`,
              total: rowCosts.dryerTotal,
            });
          }
          return totals;
        },
        {
          weight: 0,
          printHours: 0,
          dryerHours: 0,
          cost: 0,
          energy: 0,
          operating: 0,
          dryer: 0,
          materialBreakdown: [],
          energyBreakdown: [],
          operatingBreakdown: [],
          dryerBreakdown: [],
        }
      );
    };

    const updateMaterialTotals = () => {
      form.querySelectorAll("[data-collection='materials'] .collection-row").forEach(
        (row) => {
          const materialSelect = row.querySelector("[name='material_id']");
          const weightField = row.querySelector("[name='material_weight_grams']");
          const totalField = row.querySelector(".material-line-total");
          const selectedOption = materialSelect?.selectedOptions?.[0];
          const costPerKg = parseNumber(selectedOption?.dataset.costPerKg);
          const weight = parseNumber(weightField?.value);
          if (totalField) {
            totalField.value = currency(weight * (costPerKg / 1000));
          }
        }
      );
    };

    const getComponentTotals = () => {
      return Array.from(
        form.querySelectorAll("[data-collection='components'] .collection-row")
      ).reduce(
        (totals, row) => {
          const componentSelect = row.querySelector("[name='component_id']");
          const quantity = parseNumber(row.querySelector("[name='component_quantity']")?.value);
          const selectedOption = componentSelect?.selectedOptions?.[0];
          const unitCost = parseNumber(selectedOption?.dataset.unitCost);
          if (componentSelect?.value && quantity > 0) {
            totals.count += 1;
          }
          totals.cost += quantity * unitCost;
          totals.breakdown.push({
            label: selectedOption?.textContent?.trim() || "Componente",
            base: `${quantity.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} un`,
            rate: `R$ ${unitCost.toLocaleString("pt-BR", { minimumFractionDigits: 4, maximumFractionDigits: 4 })}/un`,
            total: quantity * unitCost,
          });
          return totals;
        },
        { count: 0, cost: 0, breakdown: [] }
      );
    };

    const setText = (selector, text) => {
      const element = form.querySelector(selector);
      if (element) {
        element.textContent = text;
      }
    };

    const updateInternalCostTotals = () => {
      const materialTotals = getMaterialTotals();
      const componentTotals = getComponentTotals();
      const saleTotal = Array.from(
        form.querySelectorAll("[data-collection='services'] .collection-row")
      ).reduce((total, row) => total + getServiceTotal(row), 0);
      const energyTotal = materialTotals.energy;
      const operatingTotal = materialTotals.operating;
      const dryerTotal = materialTotals.dryer;
      const laborTotal =
        parseNumber(form.querySelector("[name='labor_hours']")?.value) *
        parseNumber(form.querySelector("[name='labor_hourly_rate']")?.value);
      const designTotal =
        parseNumber(form.querySelector("[name='design_hours']")?.value) *
        parseNumber(form.querySelector("[name='design_hourly_rate']")?.value);
      const extraCost = parseNumber(form.querySelector("[name='extra_cost']")?.value);
      const totalCost =
        materialTotals.cost +
        componentTotals.cost +
        energyTotal +
        operatingTotal +
        dryerTotal +
        laborTotal +
        designTotal +
        extraCost;

      setText(".internal-material-weight-total", `${materialTotals.weight.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} g`);
      setText(".internal-material-cost-total", currency(materialTotals.cost));
      setText(".internal-component-count", `${componentTotals.count} item(ns)`);
      setText(".internal-component-cost-total", currency(componentTotals.cost));
      setText(".internal-print-hours-total", `${materialTotals.printHours.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} h`);
      setText(".internal-operating-hours-total", `${materialTotals.printHours.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} h`);
      setText(".internal-dryer-hours-total", `${materialTotals.dryerHours.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} h`);
      const energyRateField = form.querySelector("[name='energy_cost_per_hour']");
      const operatingRateField = form.querySelector("[name='operating_cost_per_hour']");
      const dryerRateField = form.querySelector("[name='dryer_cost_per_hour']");
      if (energyRateField) {
        energyRateField.value = materialTotals.printHours > 0
          ? (energyTotal / materialTotals.printHours).toFixed(4)
          : "0.0000";
      }
      if (operatingRateField) {
        operatingRateField.value = materialTotals.printHours > 0
          ? (operatingTotal / materialTotals.printHours).toFixed(4)
          : "0.0000";
      }
      if (dryerRateField) {
        dryerRateField.value = materialTotals.dryerHours > 0
          ? (dryerTotal / materialTotals.dryerHours).toFixed(4)
          : "0.0000";
      }
      setText(".internal-energy-total", currency(energyTotal));
      setText(".internal-operating-total", currency(operatingTotal));
      setText(".internal-dryer-total", currency(dryerTotal));
      setText(
        ".internal-energy-rate-display",
        `R$ ${(materialTotals.printHours > 0 ? (energyTotal / materialTotals.printHours) : 0).toLocaleString("pt-BR", { minimumFractionDigits: 4, maximumFractionDigits: 4 })}/h`
      );
      setText(
        ".internal-operating-rate-display",
        `R$ ${(materialTotals.printHours > 0 ? (operatingTotal / materialTotals.printHours) : 0).toLocaleString("pt-BR", { minimumFractionDigits: 4, maximumFractionDigits: 4 })}/h`
      );
      setText(
        ".internal-dryer-rate-display",
        `R$ ${(materialTotals.dryerHours > 0 ? (dryerTotal / materialTotals.dryerHours) : 0).toLocaleString("pt-BR", { minimumFractionDigits: 4, maximumFractionDigits: 4 })}/h`
      );
      setText(".internal-labor-total", currency(laborTotal));
      setText(".internal-design-total", currency(designTotal));
      setText(".internal-total-cost", currency(totalCost));
      setText(".internal-sale-total", currency(saleTotal));
      setText(".internal-profit-total", currency(saleTotal - totalCost));
      renderBreakdown(".internal-material-details", materialTotals.materialBreakdown);
      renderBreakdown(".internal-component-details", componentTotals.breakdown);
      renderBreakdown(".internal-energy-details", materialTotals.energyBreakdown);
      renderBreakdown(".internal-operating-details", materialTotals.operatingBreakdown);
      renderBreakdown(".internal-dryer-details", materialTotals.dryerBreakdown);
    };

    form.addEventListener("input", (event) => {
      if (
        event.target.matches(".order-quantity-field") ||
        event.target.matches("[name='service_unit_price']") ||
        event.target.matches(".service-additions") ||
        event.target.matches(".service-discounts") ||
        event.target.matches("[name='item_name']")
      ) {
        updateServiceTotals();
      }
      if (event.target.matches("[name='material_weight_grams']")) {
        updateMaterialTotals();
      }
      updateServiceTotals();
      updateMaterialTotals();
      updateInternalCostTotals();
    });

    form.addEventListener("change", (event) => {
      if (event.target.matches(".product-picker")) {
        const row = event.target.closest(".collection-row");
        const selectedOption = event.target.selectedOptions[0];
        const itemNameField = row?.querySelector("[name='item_name']");
        const categoryField = row?.querySelector(".service-category-display");
        const categorySync = row?.querySelector(".service-category-sync");
        const unitPriceField = row?.querySelector("[name='service_unit_price']");
        if (selectedOption?.value && selectedOption.value !== "__new__") {
          if (itemNameField) {
            itemNameField.value = selectedOption.dataset.name || selectedOption.textContent.trim();
          }
          if (categoryField) {
            categoryField.value = selectedOption.dataset.category || "";
          }
          if (categorySync) {
            categorySync.value = selectedOption.dataset.category || "";
          }
          if (unitPriceField) {
            unitPriceField.value = String(
              Math.max(Number(selectedOption.dataset.salePrice) || 0, 0).toFixed(2)
            );
          }
          updateServiceTotals();
        } else {
          if (categoryField) {
            categoryField.value = "";
          }
          if (categorySync) {
            categorySync.value = "";
          }
        }
      }
      if (
        event.target.matches("[name='material_id']") ||
        event.target.matches("[name='component_id']") ||
        event.target.matches("[name='printer_id']") ||
        event.target.matches("[name='filament_dryer_id']")
      ) {
        updateMaterialTotals();
      }
      updateInternalCostTotals();
    });

    updateServiceTotals();
    updateMaterialTotals();
    updateInternalCostTotals();
  });
}

function setupJobCollections() {
  const buttons = Array.from(document.querySelectorAll("[data-add-row]"));
  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const collectionName = button.dataset.addRow;
      const collection = document.querySelector(`[data-collection="${collectionName}"]`);
      if (!collection) {
        return;
      }

      const sourceRow = collection.querySelector(".collection-row");
      if (!sourceRow) {
        return;
      }

      const row = sourceRow.cloneNode(true);
      row.querySelectorAll(".searchable-select-wrapper").forEach((wrapper) => {
        const select = wrapper.querySelector("select");
        if (!select) {
          return;
        }
        select.removeAttribute("data-searchable-ready");
        select.removeAttribute("data-select-shortcut-ready");
        wrapper.replaceWith(select);
      });
      row.querySelectorAll("input, select, textarea").forEach((field) => {
        if (field.tagName === "SELECT") {
          field.selectedIndex = 0;
          return;
        }
        if (field.name === "service_name") {
          field.value = "";
          return;
        }
        if (field.name === "product_id") {
          field.selectedIndex = 0;
          return;
        }
        if (field.name === "service_category") {
          field.value = "";
          return;
        }
        if (
          field.name === "service_quantity" ||
          field.name === "component_quantity"
        ) {
          field.value = collectionName === "services" ? "1" : "0";
          return;
        }
        if (field.name === "quantity" && collectionName === "services") {
          field.value = "1";
          return;
        }
        if (
          field.name === "service_hours" ||
          field.name === "service_unit_price" ||
          field.classList.contains("service-category-display") ||
          field.type === "number"
        ) {
          field.value = field.classList.contains("service-category-display") ? "" : "0";
          return;
        }
        field.value = "";
      });

      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.className = "secondary-button remove-row-button";
      removeButton.textContent = "Remover linha";
      removeButton.addEventListener("click", () => {
        row.remove();
        collection.dispatchEvent(new Event("input", { bubbles: true }));
      });
      row.appendChild(removeButton);
      collection.appendChild(row);
      setupSelectShortcuts(row);
      setupSearchableSelects(row);
      collection.dispatchEvent(new Event("input", { bubbles: true }));
    });
  });
}

function setupJobProfitWarning() {
  const forms = Array.from(document.querySelectorAll(".job-form"));
  const parseNumber = (value) => Number(String(value || "0").replace(",", ".")) || 0;
  const currency = (value) =>
    new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
    }).format(value);

  forms.forEach((form) => {
    form.addEventListener("submit", (event) => {
      if (form.dataset.profitWarningConfirmed === "1") {
        return;
      }

      const materialCost = Array.from(
        form.querySelectorAll("[data-collection='materials'] .collection-row")
      ).reduce((total, row) => {
        const select = row.querySelector("[name='material_id']");
        const weight = parseNumber(row.querySelector("[name='material_weight_grams']")?.value);
        const selectedOption = select?.selectedOptions?.[0];
        const costPerKg = parseNumber(selectedOption?.dataset.costPerKg);
        return total + weight * (costPerKg / 1000);
      }, 0);

      const componentCost = Array.from(
        form.querySelectorAll("[data-collection='components'] .collection-row")
      ).reduce((total, row) => {
        const select = row.querySelector("[name='component_id']");
        const quantity = parseNumber(row.querySelector("[name='component_quantity']")?.value);
        const selectedOption = select?.selectedOptions?.[0];
        const unitCost = parseNumber(selectedOption?.dataset.unitCost);
        return total + quantity * unitCost;
      }, 0);

      const serviceTotal = Array.from(
        form.querySelectorAll("[data-collection='services'] .collection-row")
      ).reduce((total, row) => {
        const quantity = parseNumber(row.querySelector("[name='service_quantity']")?.value) || 1;
        const unitPrice = parseNumber(row.querySelector("[name='service_unit_price']")?.value);
        const additions = parseNumber(row.querySelector("[name='service_additions']")?.value);
        const discounts = parseNumber(row.querySelector("[name='service_discounts']")?.value);
        return total + (quantity * unitPrice) + additions - discounts;
      }, 0);

      const printHours = parseNumber(form.querySelector("[name='print_hours']")?.value);
      const energyCost = printHours * parseNumber(
        form.querySelector("[name='energy_cost_per_hour']")?.value
      );
      const operatingCost = printHours * parseNumber(
        form.querySelector("[name='operating_cost_per_hour']")?.value
      );
      const laborCost =
        parseNumber(form.querySelector("[name='labor_hours']")?.value) *
        parseNumber(form.querySelector("[name='labor_hourly_rate']")?.value);
      const designCost =
        parseNumber(form.querySelector("[name='design_hours']")?.value) *
        parseNumber(form.querySelector("[name='design_hourly_rate']")?.value);
      const extraCost = parseNumber(form.querySelector("[name='extra_cost']")?.value);
      const dryerCost =
        parseNumber(form.querySelector("[name='dryer_hours']")?.value) *
        parseNumber(form.querySelector("[name='dryer_cost_per_hour']")?.value);
      const totalCost =
        materialCost +
        componentCost +
        energyCost +
        operatingCost +
        dryerCost +
        laborCost +
        designCost +
        extraCost;

      if (serviceTotal <= 0 || totalCost <= 0) {
        return;
      }

      const profit = serviceTotal - totalCost;
      const message =
        profit <= 0
          ? `Atencao: este pedido esta com lucro de ${currency(profit)}. O valor vendido esta igual ou menor que o custo estimado. Deseja prosseguir?`
          : `Este pedido voce esta ganhando ${currency(profit)} de lucro estimado. Deseja prosseguir?`;

      if (!window.confirm(message)) {
        event.preventDefault();
        return;
      }

      form.dataset.profitWarningConfirmed = "1";
    });
  });
}

function setupProductBuilder() {
  const forms = Array.from(document.querySelectorAll(".material-form"));
  const parseNumber = (value) => Number(String(value || "0").replace(",", ".")) || 0;
  const formatDecimal = (value) => String(Math.max(Number(value) || 0, 0).toFixed(2));
  const formatCurrency = (value) =>
    new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(Math.max(Number(value) || 0, 0));
  const parseCurrency = (value) => {
    const normalized = String(value || "")
      .replace("R$", "")
      .replace(/\s/g, "")
      .trim();
    if (!normalized) {
      return 0;
    }
    if (normalized.includes(",")) {
      return Number(normalized.replace(/\./g, "").replace(",", ".")) || 0;
    }
    if (normalized.includes(".")) {
      const pieces = normalized.split(".");
      if (pieces.length === 2 && pieces[1].length <= 2) {
        return Number(normalized) || 0;
      }
      return Number(normalized.replace(/\./g, "")) || 0;
    }
    return Number(normalized) || 0;
  };

  forms.forEach((form) => {
    const materialCollection = form.querySelector("[data-collection='product-materials']");
    const componentCollection = form.querySelector("[data-collection='product-components']");
    if (!materialCollection) {
      return;
    }

    const weightField = form.querySelector("[name='weight_grams']");
    const printHoursField = form.querySelector("[name='print_hours']");
    const materialCostField = form.querySelector(".product-material-cost-total");
    const materialGrandTotal = form.querySelector(".product-material-grand-total");
    const componentGrandTotal = form.querySelector(".product-component-grand-total");
    const energyTotalField = form.querySelector(".product-energy-total");
    const addMaterialButton = form.querySelector("[data-add-row='product-materials']");
    const energyRateField = form.querySelector("[name='energy_cost_per_hour']");
    if (
      !weightField ||
      !printHoursField ||
      !materialCostField ||
      !materialGrandTotal ||
      !energyTotalField ||
      !energyRateField
    ) {
      return;
    }

    const getMaterialTotals = () => {
      const rows = Array.from(materialCollection.querySelectorAll(".collection-row"));
      return rows.reduce(
        (accumulator, row) => {
          const quantity = parseNumber(
            row.querySelector("[name='product_material_quantity']")?.value
          );
          const hours = parseNumber(
            row.querySelector("[name='product_material_print_hours']")?.value
          );
          const select = row.querySelector("[name='product_material_id']");
          const costPerKg = parseNumber(
            select?.selectedOptions?.[0]?.dataset.costPerKg
          );
          const lineTotal = quantity * (costPerKg / 1000);
          const lineField = row.querySelector(".product-material-line-total");
          if (lineField) {
            lineField.value = formatCurrency(lineTotal);
          }
          accumulator.weight += quantity;
          accumulator.hours += hours;
          accumulator.cost += lineTotal;
          return accumulator;
        },
        { weight: 0, hours: 0, cost: 0 }
      );
    };

    const getComponentTotals = () => {
      if (!componentCollection) {
        return { cost: 0 };
      }
      const rows = Array.from(componentCollection.querySelectorAll(".collection-row"));
      return rows.reduce(
        (accumulator, row) => {
          const quantity = parseNumber(
            row.querySelector("[name='product_component_quantity']")?.value
          );
          const select = row.querySelector("[name='product_component_id']");
          const unitCost = parseNumber(
            select?.selectedOptions?.[0]?.dataset.unitCost
          );
          const lineTotal = quantity * unitCost;
          const lineField = row.querySelector(".product-component-line-total");
          if (lineField) {
            lineField.value = formatCurrency(lineTotal);
          }
          accumulator.cost += lineTotal;
          return accumulator;
        },
        { cost: 0 }
      );
    };

    const updateTotals = () => {
      const materialTotals = getMaterialTotals();
      const componentTotals = getComponentTotals();
      const energyTotal = materialTotals.hours * parseCurrency(energyRateField.value);

      weightField.value = formatDecimal(materialTotals.weight);
      printHoursField.value = formatDecimal(materialTotals.hours);
      materialCostField.value = formatCurrency(materialTotals.cost);
      materialGrandTotal.textContent = `R$ ${formatCurrency(materialTotals.cost)}`;
      if (componentGrandTotal) {
        componentGrandTotal.textContent = `R$ ${formatCurrency(componentTotals.cost)}`;
      }
      energyTotalField.value = formatCurrency(energyTotal);
    };

    form.addEventListener("keydown", (event) => {
      if (
        event.target.matches("[name='product_material_print_hours']") &&
        event.key === "Tab" &&
        !event.shiftKey &&
        addMaterialButton
      ) {
        event.preventDefault();
        addMaterialButton.focus();
      }
    });

    form.addEventListener("input", (event) => {
      if (
        event.target.matches("[name='product_material_quantity']") ||
        event.target.matches("[name='product_material_print_hours']") ||
        event.target.matches("[name='product_component_quantity']") ||
        event.target.matches("[name='energy_cost_per_hour']")
      ) {
        updateTotals();
      }
    });

    form.addEventListener("change", (event) => {
      if (
        event.target.matches("[name='product_material_quantity']") ||
        event.target.matches("[name='product_material_print_hours']") ||
        event.target.matches("[name='product_material_id']") ||
        event.target.matches("[name='product_component_id']") ||
        event.target.matches("[name='product_component_quantity']") ||
        event.target.matches("[name='energy_cost_per_hour']")
      ) {
        updateTotals();
      }
    });

    materialCollection.addEventListener("input", updateTotals);
    if (componentCollection) {
      componentCollection.addEventListener("input", updateTotals);
    }
    updateTotals();
  });
}

function setupCommercialEntries() {
  const forms = Array.from(document.querySelectorAll(".commercial-entry-form"));
  const parseNumber = (value) => Number(String(value || "0").replace(",", ".")) || 0;
  const parseCurrency = (value) => {
    const normalized = String(value || "")
      .replace("R$", "")
      .replace(/\s/g, "")
      .trim();
    if (!normalized) {
      return 0;
    }
    if (normalized.includes(",")) {
      return Number(normalized.replace(/\./g, "").replace(",", ".")) || 0;
    }
    return Number(normalized) || 0;
  };
  const formatCurrency = (value) =>
    new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(Math.max(Number(value) || 0, 0));

  forms.forEach((form) => {
    const typeField = form.querySelector("[name='item_type']");
    const codeField = form.querySelector("[name='item_ref']");
    const brandField = form.querySelector("[name='brand_name_display']");
    const descriptionField = form.querySelector("[name='line_description_display']");
    const colorField =
      form.querySelector("[name='color_name_display']") ||
      form.querySelector("[name='color_name']");
    const unitField = form.querySelector("[name='unit_name_display']");
    const quantityField = form.querySelector("[name='quantity']");
    const amountField = form.querySelector("[name='amount']");
    const freightField = form.querySelector("[name='freight']");
    const taxField = form.querySelector("[name='tax']");
    const discountField = form.querySelector("[name='discount']");
    const totalField = form.querySelector("[name='total_amount_display']");
    const unitCostField = form.querySelector("[name='unit_cost_display']");
    const siteField = form.querySelector("[name='site']");

    if (!codeField || !brandField || !descriptionField || !colorField || !totalField || !unitCostField) {
      return;
    }

    const updateItemDetails = () => {
      const selectedOption = codeField.selectedOptions[0];
      if (!selectedOption || !selectedOption.value) {
        brandField.value = "";
        descriptionField.value = "";
        colorField.value = "";
        if (unitField) {
          unitField.value = "";
        }
        return;
      }

      if (typeField) {
        typeField.value = selectedOption.dataset.itemType || "";
      }
      brandField.value = selectedOption.dataset.brandName || "";
      descriptionField.value = selectedOption.dataset.lineDescription || "";
      colorField.value = selectedOption.dataset.colorName || "";
      if (unitField) {
        unitField.value = selectedOption.dataset.unitName || "";
      }
      if (siteField && !siteField.value) {
        siteField.value = selectedOption.dataset.site || "";
      }
    };

    const updateTotals = () => {
      const quantity = parseNumber(quantityField?.value);
      const amount = parseCurrency(amountField?.value);
      const freight = parseCurrency(freightField?.value);
      const tax = parseCurrency(taxField?.value);
      const discount = parseCurrency(discountField?.value);
      const total = Math.max(amount + freight + tax - discount, 0);
      const unitCost = quantity > 0 ? total / quantity : 0;
      totalField.value = formatCurrency(total);
      unitCostField.value = formatCurrency(unitCost);
    };

    codeField.addEventListener("change", () => {
      if (codeField.value === "__new_component__") {
        saveFormDraft(form);
        codeField.value = "";
        window.location.href = codeField.dataset.componentUrl;
        return;
      }
      if (codeField.value === "__new_material__") {
        saveFormDraft(form);
        codeField.value = "";
        window.location.href = codeField.dataset.materialUrl;
        return;
      }
      updateItemDetails();
      updateTotals();
    });
    [quantityField, amountField, freightField, taxField, discountField].forEach((field) => {
      field?.addEventListener("input", updateTotals);
      field?.addEventListener("change", updateTotals);
    });

    updateItemDetails();
    updateTotals();
  });
}

function setupCustomerPostalCodeAutofill() {
  const forms = Array.from(document.querySelectorAll(".customer-registry-form"));
  forms.forEach((form) => {
    const postalCodeField = form.querySelector("[name='postal_code']");
    const streetField = form.querySelector("[name='street']");
    const neighborhoodField = form.querySelector("[name='neighborhood']");
    const cityField = form.querySelector("[name='city']");
    const stateField = form.querySelector("[name='state']");
    if (!postalCodeField) {
      return;
    }

    let lastFetchedPostalCode = "";

    const normalizePostalCode = (value) => String(value || "").replace(/\D/g, "").slice(0, 8);

    const applyPostalCodeMask = () => {
      const digits = normalizePostalCode(postalCodeField.value);
      postalCodeField.value = digits.length > 5 ? `${digits.slice(0, 5)}-${digits.slice(5)}` : digits;
      return digits;
    };

    const maybeFillField = (field, value) => {
      if (!field || !value) {
        return;
      }
      if (!String(field.value || "").trim()) {
        field.value = value;
      }
    };

    const fetchAddress = async () => {
      const postalCode = applyPostalCodeMask();
      if (postalCode.length !== 8 || postalCode === lastFetchedPostalCode) {
        return;
      }
      lastFetchedPostalCode = postalCode;
      try {
        const response = await fetch(`https://viacep.com.br/ws/${postalCode}/json/`);
        if (!response.ok) {
          return;
        }
        const data = await response.json();
        if (data.erro) {
          return;
        }
        maybeFillField(streetField, data.logradouro || "");
        maybeFillField(neighborhoodField, data.bairro || "");
        maybeFillField(cityField, data.localidade || "");
        maybeFillField(stateField, data.uf || "");
      } catch (_error) {
        // Manual entry remains available if CEP lookup fails.
      }
    };

    postalCodeField.addEventListener("input", () => {
      applyPostalCodeMask();
      if (normalizePostalCode(postalCodeField.value).length < 8) {
        lastFetchedPostalCode = "";
      }
    });
    postalCodeField.addEventListener("blur", fetchAddress);
  });
}

function setupAutoUppercaseFields() {
  const fields = Array.from(document.querySelectorAll(".auto-uppercase"));
  fields.forEach((field) => {
    if (field.dataset.uppercaseReady === "1") {
      return;
    }
    field.dataset.uppercaseReady = "1";
    const applyUppercase = () => {
      if (!("value" in field) || field.readOnly || field.disabled) {
        return;
      }
      field.value = String(field.value || "").toUpperCase();
    };
    field.addEventListener("input", applyUppercase);
    field.addEventListener("blur", applyUppercase);
    applyUppercase();
  });
}

function setupServiceWorker() {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/static/service-worker.js").catch(() => {
      // PWA support is optional during local development.
    });
  });
}

setupSupplierShortcut();
setupSelectShortcuts();
setupSearchableSelects();
setupCurrencyFields();
setupDateMasks();
setupCommercialDateDefaults();
setupPrinterHourlyCost();
setupJobPrinterCosts();
setupJobDryerCosts();
setupJobInlineTotals();
setupJobCollections();
setupJobProfitWarning();
setupProductBuilder();
setupCommercialEntries();
setupCustomerPostalCodeAutofill();
setupAutoUppercaseFields();
setupFormDraftPersistence();
setupPendingSelections();
setupScrollRestore();
setupMaterialsCatalogClientFiltering();
setupAutoFilterForms();
setupHeaderFilterPopovers();
setupMaterialsCatalogAnchor();
setupServiceWorker();
