import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.*;
import javax.swing.tree.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.zip.*;

public class BibleReaderApp extends JFrame {
    private static final long serialVersionUID = 1L;
    private static final File DATA_FILE = new File("bible_reader_data.ser");
    private static final File BACKUP_DIR = new File("backups");

    private static final String OFFICIAL_BSB_USFM_ZIP =
            "https://github.com/BSB-publishing/bsb2usfm/releases/latest/download/BSB_usfm.zip";

    private static final String MORPHGNT_ZIP =
            "https://zenodo.org/records/376200/files/morphgnt%2Fsblgnt-6.12.zip?download=1";

    private final Color darkRed = new Color(92, 23, 23);
    private final Color cream = new Color(255, 253, 248);
    private final Color panelBg = new Color(248, 244, 236);
    private final Color noteYellow = new Color(255, 244, 150);
    private final Color categoryBlue = new Color(211, 233, 255);
    private final Color linkPurple = new Color(231, 218, 255);
    private final Color questionRed = new Color(255, 214, 214);
    private final Color greekGreen = new Color(218, 245, 218);

    private AppData data;
    private Profile currentProfile;

    private CardLayout cards;
    private JPanel cardPanel;
    private JLabel statusLabel;
    private JLabel profileLabel;
    private JLabel sourceLabel;
    private JComboBox<String> profileBox;
    private JComboBox<String> bookCombo;
    private JComboBox<Integer> chapterCombo;

    private DefaultMutableTreeNode rootNode;
    private DefaultTreeModel treeModel;
    private JTree libraryTree;

    private JTextPane readerPane;
    private JPopupMenu selectionActionPopup;
    private Point readerSelectionPressPoint;
    private boolean readerSelectionDragged = false;
    private JPanel detailsPanel;
    private JTextArea importLog;

    private JTextField searchField;
    private DefaultListModel<String> searchModel;
    private JList<String> searchList;

    private DefaultListModel<String> categoryModel;
    private JList<String> categoryList;

    private DefaultListModel<String> questionModel;
    private JList<String> questionList;

    private JPanel sideSearchBody;
    private JButton sideSearchToggleBtn;
    private JTextField sideSearchField;
    private DefaultListModel<String> sideSearchModel;
    private JList<String> sideSearchList;
    private JTextArea sideSearchPreview;
    private boolean sideSearchExpanded = true;

    private String selectedBook = "";
    private int selectedChapter = 1;
    private String currentSourceKey = "";
    private String currentSourceTitle = "";
    private boolean refreshingUi = false;
    private boolean loadingReader = false;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (Exception ignored) {}

            new BibleReaderApp().setVisible(true);
        });
    }

    public BibleReaderApp() {
        super("Bible Study Library - Phrase Notes");
        data = loadData();
        repairLoadedData();

        if (data.profiles.isEmpty()) {
            data.profiles.put("Default Study", new Profile("Default Study"));
        }

        currentProfile = data.profiles.values().iterator().next();
        buildUi();
        refreshEverything();

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1450, 880);
        setLocationRelativeTo(null);
    }

    private void buildUi() {
        setLayout(new BorderLayout());
        getContentPane().setBackground(panelBg);

        JPanel top = new JPanel(new BorderLayout(12, 8));
        top.setBackground(darkRed);
        top.setBorder(new EmptyBorder(10, 12, 10, 12));

        JPanel titlePanel = new JPanel(new GridLayout(2, 1));
        titlePanel.setOpaque(false);

        JLabel title = new JLabel("Bible Study Library");
        title.setForeground(Color.WHITE);
        title.setFont(new Font("Segoe UI", Font.BOLD, 25));

        profileLabel = new JLabel(" ");
        profileLabel.setForeground(new Color(255, 230, 230));
        profileLabel.setFont(new Font("Segoe UI", Font.PLAIN, 13));

        titlePanel.add(title);
        titlePanel.add(profileLabel);

        JPanel nav = new JPanel(new FlowLayout(FlowLayout.RIGHT, 7, 0));
        nav.setOpaque(false);

        profileBox = new JComboBox<>();
        profileBox.setPreferredSize(new Dimension(190, 30));
        profileBox.addActionListener(e -> switchProfile());

        JButton newProfile = navButton("New Profile");
        JButton study = navButton("Study");
        JButton importBtn = navButton("Import");
        JButton search = navButton("Search");
        JButton categories = navButton("Categories");
        JButton questions = navButton("Questions");
        JButton backup = navButton("Backup");
        JButton export = navButton("Export");

        newProfile.addActionListener(e -> createProfile());
        study.addActionListener(e -> showCard("study"));
        importBtn.addActionListener(e -> showCard("import"));
        search.addActionListener(e -> showCard("search"));
        categories.addActionListener(e -> { refreshCategories(); showCard("categories"); });
        questions.addActionListener(e -> { refreshQuestions(); showCard("questions"); });
        backup.addActionListener(e -> backupNow());
        export.addActionListener(e -> exportNotes());

        nav.add(labelWhite("Profile:"));
        nav.add(profileBox);
        nav.add(newProfile);
        nav.add(study);
        nav.add(importBtn);
        nav.add(search);
        nav.add(categories);
        nav.add(questions);
        nav.add(backup);
        nav.add(export);

        top.add(titlePanel, BorderLayout.WEST);
        top.add(nav, BorderLayout.EAST);
        add(top, BorderLayout.NORTH);

        cards = new CardLayout();
        cardPanel = new JPanel(cards);
        cardPanel.add(buildStudyPage(), "study");
        cardPanel.add(buildImportPage(), "import");
        cardPanel.add(buildSearchPage(), "search");
        cardPanel.add(buildCategoriesPage(), "categories");
        cardPanel.add(buildQuestionsPage(), "questions");
        add(cardPanel, BorderLayout.CENTER);

        statusLabel = new JLabel(" Ready");
        statusLabel.setBorder(new EmptyBorder(5, 10, 5, 10));
        add(statusLabel, BorderLayout.SOUTH);
    }

    private JPanel buildStudyPage() {
        JPanel page = new JPanel(new BorderLayout());
        page.setBackground(panelBg);

        JSplitPane main = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        main.setResizeWeight(0.22);
        main.setDividerSize(7);
        main.setLeftComponent(buildLibraryPanel());

        JSplitPane centerRight = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        centerRight.setResizeWeight(0.67);
        centerRight.setDividerSize(7);
        centerRight.setLeftComponent(buildReaderPanel());
        centerRight.setRightComponent(buildRightSidebar());

        main.setRightComponent(centerRight);
        page.add(main, BorderLayout.CENTER);
        return page;
    }

    private JPanel buildLibraryPanel() {
        JPanel p = new JPanel(new BorderLayout(8, 8));
        p.setBorder(new EmptyBorder(10, 10, 10, 10));
        p.setBackground(panelBg);

        JLabel h = new JLabel("Library");
        h.setFont(new Font("Segoe UI", Font.BOLD, 20));
        h.setForeground(darkRed);

        rootNode = new DefaultMutableTreeNode("Library");
        treeModel = new DefaultTreeModel(rootNode);
        libraryTree = new JTree(treeModel);
        libraryTree.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        libraryTree.addTreeSelectionListener(e -> onTreeSelected());

        p.add(h, BorderLayout.NORTH);
        p.add(new JScrollPane(libraryTree), BorderLayout.CENTER);
        return p;
    }

    private JPanel buildReaderPanel() {
        JPanel p = new JPanel(new BorderLayout(8, 8));
        p.setBorder(new EmptyBorder(10, 10, 10, 10));
        p.setBackground(cream);

        JPanel nav = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        nav.setOpaque(false);

        bookCombo = new JComboBox<>();
        bookCombo.setPreferredSize(new Dimension(180, 30));

        chapterCombo = new JComboBox<>();
        chapterCombo.setPreferredSize(new Dimension(90, 30));

        sourceLabel = new JLabel("No Bible imported yet");
        sourceLabel.setForeground(new Color(100, 70, 55));

        bookCombo.addActionListener(e -> {
            if (refreshingUi) return;
            Object o = bookCombo.getSelectedItem();
            if (o != null) {
                selectedBook = o.toString();
                refreshChapterCombo();
                showSelectedChapter(true);
            }
        });

        chapterCombo.addActionListener(e -> {
            if (refreshingUi) return;
            Object o = chapterCombo.getSelectedItem();
            if (o instanceof Integer) {
                selectedChapter = (Integer) o;
                showSelectedChapter(true);
            }
        });

        nav.add(new JLabel("Book:"));
        nav.add(bookCombo);
        nav.add(new JLabel("Chapter:"));
        nav.add(chapterCombo);
        nav.add(sourceLabel);

        readerPane = new JTextPane() {
            public String getToolTipText(MouseEvent e) {
                int pos = viewToModel2D(e.getPoint());
                TextAnnotation a = annotationAt(pos);
                if (a == null) return null;
                return "<html><b>" + esc(a.type) + "</b>" +
                        (a.category.isEmpty() ? "" : "<br>Category: " + esc(a.category)) +
                        (a.target.isEmpty() ? "" : "<br>Attached: " + esc(a.target)) +
                        "<br>" + esc(shorten(a.note, 240)).replace("\n", "<br>") + "</html>";
            }
        };

        ToolTipManager.sharedInstance().registerComponent(readerPane);
        readerPane.setEditable(false);
        readerPane.setFont(new Font("Georgia", Font.PLAIN, 17));
        readerPane.setMargin(new Insets(14, 16, 14, 16));
        readerPane.setBackground(cream);
        readerPane.addMouseListener(new MouseAdapter() {
            public void mousePressed(MouseEvent e) {
                hideSelectionActionPopup();
                if (SwingUtilities.isLeftMouseButton(e)) {
                    readerSelectionPressPoint = e.getPoint();
                    readerSelectionDragged = false;
                }
                if (e.isPopupTrigger()) showReaderMenu(e);
            }
            public void mouseReleased(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    hideSelectionActionPopup();
                    showReaderMenu(e);
                    return;
                }
                if (SwingUtilities.isLeftMouseButton(e) && readerSelectionDragged && e.getClickCount() == 1) {
                    showSelectionActionPopupIfNeeded();
                }
            }
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e)) handleReaderLeftClick(e);
                if (e.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(e)) {
                    hideSelectionActionPopup();
                    showQuickNoteForSelectionOrWord(e);
                }
            }
        });
        readerPane.addMouseMotionListener(new MouseMotionAdapter() {
            public void mouseDragged(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && readerSelectionPressPoint != null
                        && readerSelectionPressPoint.distance(e.getPoint()) > 4) {
                    readerSelectionDragged = true;
                }
            }
        });
        readerPane.addCaretListener(e -> {
            if (readerPane.getSelectionEnd() <= readerPane.getSelectionStart()) hideSelectionActionPopup();
        });

        p.add(nav, BorderLayout.NORTH);
        p.add(new JScrollPane(readerPane), BorderLayout.CENTER);
        return p;
    }

    private JPanel buildRightSidebar() {
        JPanel wrapper = new JPanel(new BorderLayout(8, 8));
        wrapper.setBackground(panelBg);
        wrapper.add(buildSideSearchPanel(), BorderLayout.NORTH);
        wrapper.add(buildDetailsPanel(), BorderLayout.CENTER);
        return wrapper;
    }

    private JPanel buildDetailsPanel() {
        JPanel p = new JPanel(new BorderLayout(8, 8));
        p.setBorder(new EmptyBorder(10, 10, 10, 10));
        p.setBackground(panelBg);

        JLabel h = new JLabel("Notes / Attachments");
        h.setFont(new Font("Segoe UI", Font.BOLD, 20));
        h.setForeground(darkRed);

        detailsPanel = new JPanel();
        detailsPanel.setLayout(new BoxLayout(detailsPanel, BoxLayout.Y_AXIS));
        detailsPanel.setBackground(cream);
        detailsPanel.setBorder(new EmptyBorder(10, 10, 10, 10));

        p.add(h, BorderLayout.NORTH);
        p.add(new JScrollPane(detailsPanel), BorderLayout.CENTER);
        return p;
    }

    private JPanel buildSideSearchPanel() {
        JPanel outer = new JPanel(new BorderLayout(6, 6));
        outer.setBorder(new CompoundBorder(
                new EmptyBorder(10, 10, 0, 10),
                new CompoundBorder(new LineBorder(new Color(180, 145, 135)), new EmptyBorder(6, 6, 6, 6))
        ));
        outer.setBackground(panelBg);

        JPanel header = new JPanel(new BorderLayout(6, 6));
        header.setOpaque(false);

        JLabel title = new JLabel("Quick Search");
        title.setFont(new Font("Segoe UI", Font.BOLD, 14));
        title.setForeground(darkRed);

        sideSearchToggleBtn = blackButton("Minimize");
        sideSearchToggleBtn.addActionListener(e -> toggleSideSearch());

        header.add(title, BorderLayout.WEST);
        header.add(sideSearchToggleBtn, BorderLayout.EAST);

        sideSearchBody = new JPanel(new BorderLayout(6, 6));
        sideSearchBody.setOpaque(false);

        JPanel inputRow = new JPanel(new BorderLayout(5, 5));
        inputRow.setOpaque(false);

        sideSearchField = new JTextField();
        sideSearchField.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        sideSearchField.addActionListener(e -> doSideSearch());

        JButton go = blackButton("Go");
        go.addActionListener(e -> doSideSearch());

        inputRow.add(sideSearchField, BorderLayout.CENTER);
        inputRow.add(go, BorderLayout.EAST);

        sideSearchModel = new DefaultListModel<>();
        sideSearchList = new JList<>(sideSearchModel);
        sideSearchList.setFont(new Font("Consolas", Font.PLAIN, 12));
        sideSearchList.setVisibleRowCount(6);
        sideSearchList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isRightMouseButton(e)) {
                    int idx = sideSearchList.locationToIndex(e.getPoint());
                    if (idx >= 0) sideSearchList.setSelectedIndex(idx);
                    showSideSearchMenu(e);
                    return;
                }

                if (SwingUtilities.isLeftMouseButton(e)) {
                    previewSideSearchResult();
                    if (e.getClickCount() == 2) showFullViewForSideSearchResult();
                }
            }
            public void mousePressed(MouseEvent e) { if (e.isPopupTrigger()) showSideSearchMenu(e); }
            public void mouseReleased(MouseEvent e) { if (e.isPopupTrigger()) showSideSearchMenu(e); }
        });

        sideSearchPreview = readonlyArea();
        sideSearchPreview.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        sideSearchPreview.setText("Search Bible verses, Greek entries, imported books, highlights, and questions here without leaving the study screen. Click a result to preview it. Right-click for full view.");

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(sideSearchList), new JScrollPane(sideSearchPreview));
        split.setResizeWeight(0.48);
        split.setDividerSize(5);
        split.setPreferredSize(new Dimension(260, 260));

        sideSearchBody.add(inputRow, BorderLayout.NORTH);
        sideSearchBody.add(split, BorderLayout.CENTER);

        outer.add(header, BorderLayout.NORTH);
        outer.add(sideSearchBody, BorderLayout.CENTER);
        return outer;
    }

    private JPanel buildImportPage() {
        JPanel page = new JPanel(new BorderLayout(12, 12));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Import Library Data");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JTextArea instructions = readonlyArea();
        instructions.setText(
                "Import the Bible and philosophy/public-domain works here.\n\n" +
                "Bible options:\n" +
                "• Download + Import Official BSB USFM\n" +
                "• Import BSB/USFM ZIP or Folder\n" +
                "• Import Bible CSV: Book,Chapter,Verse,Text\n\n" +
                "Greek options:\n" +
                "• Download + Import MorphGNT Greek\n" +
                "• Import MorphGNT ZIP / TXT Folder\n" +
                "• Import Greek CSV: Book,Chapter,Verse,GreekText,Details\n" +
                "After Greek is imported, right-click a verse number and choose 'Check Greek For This Verse'.\n\n" +
                "Philosophy / other works:\n" +
                "• Import TXT, then select exact words or paragraphs in the reader.\n" +
                "• Right-click selected text to add notes, categories, questions, or attachments.\n" +
                "• Attachments can point to a Bible verse like Romans 14:13 or to a unique chunk inside a selected book.\n\n" +
                "Hover over highlighted text to preview its note. Click highlighted text to see actions in the right panel."
        );

        JPanel actions = new JPanel(new GridLayout(0, 1, 10, 10));
        actions.setOpaque(false);
        actions.setPreferredSize(new Dimension(360, 1));

        JButton downloadBsb = blackButton("Download + Import Official BSB USFM");
        JButton importUsfm = blackButton("Import BSB/USFM ZIP or Folder");
        JButton importBibleCsv = blackButton("Import Bible CSV");
        JButton downloadGreek = blackButton("Download + Import MorphGNT Greek");
        JButton importGreekZip = blackButton("Import MorphGNT ZIP / TXT Folder");
        JButton importGreekCsv = blackButton("Import Greek CSV");
        JButton importTxt = blackButton("Import Philosophy / Public Domain TXT");
        JButton templates = blackButton("Create Import Templates");
        JButton clearBible = blackButton("Clear Bible Text Only");

        downloadBsb.addActionListener(e -> downloadAndImportBsbUsfm());
        importUsfm.addActionListener(e -> importUsfmZipOrFolder());
        importBibleCsv.addActionListener(e -> importBibleCsv());
        downloadGreek.addActionListener(e -> downloadAndImportMorphGnt());
        importGreekZip.addActionListener(e -> importMorphGntZipOrFolder());
        importGreekCsv.addActionListener(e -> importGreekCsv());
        importTxt.addActionListener(e -> importLibraryText());
        templates.addActionListener(e -> createTemplates());
        clearBible.addActionListener(e -> clearBibleText());

        actions.add(downloadBsb);
        actions.add(importUsfm);
        actions.add(importBibleCsv);
        actions.add(downloadGreek);
        actions.add(importGreekZip);
        actions.add(importGreekCsv);
        actions.add(importTxt);
        actions.add(templates);
        actions.add(clearBible);

        importLog = new JTextArea();
        importLog.setEditable(false);
        importLog.setFont(new Font("Consolas", Font.PLAIN, 13));
        importLog.setLineWrap(true);
        importLog.setWrapStyleWord(true);
        importLog.setText("Import log will appear here.\n");

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(instructions), new JScrollPane(importLog));
        split.setResizeWeight(0.55);
        split.setDividerSize(7);

        page.add(h, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        page.add(actions, BorderLayout.EAST);
        return page;
    }

    private JPanel buildSearchPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Search Everything");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JPanel top = new JPanel(new BorderLayout(8, 8));
        top.setOpaque(false);

        searchField = new JTextField();
        searchField.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        searchField.addActionListener(e -> doSearch());

        JButton btn = blackButton("Search");
        btn.addActionListener(e -> doSearch());

        top.add(searchField, BorderLayout.CENTER);
        top.add(btn, BorderLayout.EAST);

        searchModel = new DefaultListModel<>();
        searchList = new JList<>(searchModel);
        searchList.setFont(new Font("Consolas", Font.PLAIN, 13));
        searchList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) openSearchResult();
            }
        });

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(top, BorderLayout.SOUTH);

        page.add(north, BorderLayout.NORTH);
        page.add(new JScrollPane(searchList), BorderLayout.CENTER);
        return page;
    }

    private JPanel buildCategoriesPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Categories");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.setOpaque(false);

        JButton add = blackButton("Create Category");
        JButton view = blackButton("View Selected");
        JButton color = blackButton("Change Highlight Color");

        add.addActionListener(e -> createCategory(null));
        view.addActionListener(e -> viewSelectedCategory());
        color.addActionListener(e -> changeSelectedCategoryColor());

        top.add(add);
        top.add(view);
        top.add(color);

        categoryModel = new DefaultListModel<>();
        categoryList = new JList<>(categoryModel);
        categoryList.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        categoryList.setCellRenderer(new CategoryCellRenderer());
        categoryList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) viewSelectedCategory();
            }
        });

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(top, BorderLayout.SOUTH);

        page.add(north, BorderLayout.NORTH);
        page.add(new JScrollPane(categoryList), BorderLayout.CENTER);
        return page;
    }

    private JPanel buildQuestionsPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Unfinished Questions");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.setOpaque(false);

        JButton toggle = blackButton("Toggle Answered / Unanswered");
        JButton add = blackButton("Add Question To Current Selection");
        toggle.addActionListener(e -> toggleSelectedQuestion());
        add.addActionListener(e -> addQuestionForSelection());

        top.add(toggle);
        top.add(add);

        questionModel = new DefaultListModel<>();
        questionList = new JList<>(questionModel);
        questionList.setFont(new Font("Segoe UI", Font.PLAIN, 15));

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(top, BorderLayout.SOUTH);

        page.add(north, BorderLayout.NORTH);
        page.add(new JScrollPane(questionList), BorderLayout.CENTER);
        return page;
    }

    private JButton navButton(String s) { return styledButton(s, new Color(255, 248, 240), Color.BLACK); }
    private JButton blackButton(String s) { return styledButton(s, new Color(255, 248, 240), Color.BLACK); }

    private JButton styledButton(String s, Color bg, Color fg) {
        JButton b = new JButton(s);
        b.setFont(new Font("Segoe UI", Font.BOLD, 12));
        b.setFocusPainted(false);
        b.setBackground(bg);
        b.setForeground(fg);
        b.setBorder(new CompoundBorder(new LineBorder(new Color(120, 60, 60)), new EmptyBorder(6, 10, 6, 10)));
        return b;
    }

    private JLabel labelWhite(String s) {
        JLabel l = new JLabel(s);
        l.setForeground(Color.WHITE);
        l.setFont(new Font("Segoe UI", Font.BOLD, 12));
        return l;
    }

    private JTextArea readonlyArea() {
        JTextArea a = new JTextArea();
        a.setEditable(false);
        a.setLineWrap(true);
        a.setWrapStyleWord(true);
        a.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        a.setBackground(cream);
        a.setBorder(new EmptyBorder(10, 10, 10, 10));
        return a;
    }

    private void showCard(String name) {
        cards.show(cardPanel, name);
    }

    private void log(String s) {
        Runnable r = () -> {
            if (importLog != null) {
                importLog.append(new SimpleDateFormat("HH:mm:ss").format(new Date()) + " - " + s + "\n");
                importLog.setCaretPosition(importLog.getDocument().getLength());
            }
            if (statusLabel != null) statusLabel.setText(" " + s);
        };
        if (SwingUtilities.isEventDispatchThread()) r.run();
        else SwingUtilities.invokeLater(r);
    }

    private void refreshEverything() {
        refreshingUi = true;
        try {
            refreshProfiles();
            refreshLibraryTree();
            refreshBookCombo();
            refreshCategories();
            refreshQuestions();
            updateHeader();
        } finally {
            refreshingUi = false;
        }

        if (!selectedBook.isEmpty() && data.bible.containsKey(selectedBook)) showSelectedChapter(false);
        else if (!data.bible.isEmpty()) {
            selectedBook = orderedBooks().get(0);
            selectedChapter = data.getChapters(selectedBook).iterator().next();
            showSelectedChapter(false);
        } else {
            showBlankReader();
        }
    }

    private void updateHeader() {
        if (profileLabel == null || currentProfile == null) return;
        profileLabel.setText("Profile: " + currentProfile.name +
                " | Bible verses: " + data.totalVerseCount() +
                " | Greek entries: " + data.greek.size() +
                " | Library works: " + data.libraryDocs.size() +
                " | Text notes: " + currentProfile.annotations.size() +
                " | Unfinished questions: " + countUnanswered());
    }

    private void refreshProfiles() {
        if (profileBox == null) return;
        profileBox.removeAllItems();
        for (String n : data.profiles.keySet()) profileBox.addItem(n);
        if (currentProfile != null) profileBox.setSelectedItem(currentProfile.name);
    }

    private void switchProfile() {
        if (refreshingUi) return;
        Object o = profileBox.getSelectedItem();
        if (o == null) return;
        Profile p = data.profiles.get(o.toString());
        if (p != null && p != currentProfile) {
            currentProfile = p;
            repairProfile(currentProfile);
            refreshEverything();
        }
    }

    private void createProfile() {
        String n = JOptionPane.showInputDialog(this, "Profile name:");
        if (n == null) return;
        n = n.trim();
        if (n.isEmpty()) return;
        if (data.profiles.containsKey(n)) {
            JOptionPane.showMessageDialog(this, "That profile already exists.");
            return;
        }
        Profile p = new Profile(n);
        data.profiles.put(n, p);
        currentProfile = p;
        saveData();
        refreshEverything();
    }

    private void refreshLibraryTree() {
        if (rootNode == null) return;
        rootNode.removeAllChildren();

        DefaultMutableTreeNode bible = new DefaultMutableTreeNode("Bible");
        for (String book : orderedBooks()) {
            DefaultMutableTreeNode bn = new DefaultMutableTreeNode(book);
            for (Integer ch : data.getChapters(book)) {
                String key = "BIBLE:" + book + " " + ch;
                int visits = currentProfile.visitCounts.getOrDefault(key, 0);
                bn.add(new DefaultMutableTreeNode("Chapter " + ch + (visits > 0 ? " (" + visits + " visits)" : "")));
            }
            bible.add(bn);
        }

        DefaultMutableTreeNode philosophy = new DefaultMutableTreeNode("Philosophy / Other");
        for (LibraryDoc d : data.libraryDocs) philosophy.add(new DefaultMutableTreeNode(d.title));

        rootNode.add(bible);
        rootNode.add(philosophy);
        treeModel.reload();
        for (int i = 0; i < libraryTree.getRowCount(); i++) libraryTree.expandRow(i);
    }

    private void onTreeSelected() {
        if (refreshingUi) return;
        TreePath path = libraryTree.getSelectionPath();
        if (path == null) return;
        Object[] parts = path.getPath();

        if (parts.length >= 3 && "Bible".equals(parts[1].toString())) {
            selectedBook = parts[2].toString();
            if (parts.length >= 4) {
                String num = parts[3].toString().replace("Chapter ", "").split("\\s+")[0];
                try { selectedChapter = Integer.parseInt(num); } catch (Exception ignored) {}
            }
            refreshBookCombo();
            showSelectedChapter(true);
            showCard("study");
        } else if (parts.length >= 3 && "Philosophy / Other".equals(parts[1].toString())) {
            showLibraryDoc(parts[2].toString());
            showCard("study");
        }
    }

    private List<String> orderedBooks() {
        List<String> list = new ArrayList<>(data.bible.keySet());
        Map<String, Integer> order = bibleBookOrder();
        list.sort(Comparator.comparingInt((String b) -> order.getOrDefault(b.toLowerCase(Locale.ROOT), 999)).thenComparing(b -> b));
        return list;
    }

    private Map<String, Integer> bibleBookOrder() {
        String[] a = {
                "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy", "Joshua", "Judges", "Ruth",
                "1 Samuel", "2 Samuel", "1 Kings", "2 Kings", "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther",
                "Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Solomon", "Isaiah", "Jeremiah", "Lamentations", "Ezekiel", "Daniel",
                "Hosea", "Joel", "Amos", "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah", "Haggai", "Zechariah", "Malachi",
                "Matthew", "Mark", "Luke", "John", "Acts", "Romans", "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians", "Philippians", "Colossians",
                "1 Thessalonians", "2 Thessalonians", "1 Timothy", "2 Timothy", "Titus", "Philemon", "Hebrews", "James", "1 Peter", "2 Peter",
                "1 John", "2 John", "3 John", "Jude", "Revelation"
        };
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < a.length; i++) m.put(a[i].toLowerCase(Locale.ROOT), i + 1);
        return m;
    }

    private void refreshBookCombo() {
        if (bookCombo == null) return;
        refreshingUi = true;
        try {
            Object selected = bookCombo.getSelectedItem();
            bookCombo.removeAllItems();
            for (String b : orderedBooks()) bookCombo.addItem(b);
            if (selectedBook == null || selectedBook.isEmpty() || !data.bible.containsKey(selectedBook)) {
                if (selected != null && data.bible.containsKey(selected.toString())) selectedBook = selected.toString();
                else if (bookCombo.getItemCount() > 0) selectedBook = bookCombo.getItemAt(0);
            }
            bookCombo.setSelectedItem(selectedBook);
            refreshChapterCombo();
        } finally {
            refreshingUi = false;
        }
    }

    private void refreshChapterCombo() {
        if (chapterCombo == null) return;
        refreshingUi = true;
        try {
            chapterCombo.removeAllItems();
            if (selectedBook == null || selectedBook.isEmpty()) return;
            for (Integer ch : data.getChapters(selectedBook)) chapterCombo.addItem(ch);
            if (!data.getChapters(selectedBook).contains(selectedChapter) && chapterCombo.getItemCount() > 0) {
                selectedChapter = chapterCombo.getItemAt(0);
            }
            chapterCombo.setSelectedItem(selectedChapter);
        } finally {
            refreshingUi = false;
        }
    }

    private void showBlankReader() {
        loadingReader = true;
        try {
            currentSourceKey = "";
            currentSourceTitle = "";
            readerPane.setText("No Bible text loaded yet. Click Import, then download/import BSB USFM or import a Bible CSV.\n\nYou can also import TXT files for philosophy works and annotate selected phrases inside them.");
            sourceLabel.setText("No source loaded");
            showDetailsText("Select text and right-click to add a note, category, question, or attachment.");
        } finally {
            loadingReader = false;
        }
    }

    private void showSelectedChapter(boolean countVisit) {
        if (readerPane == null || selectedBook == null || selectedBook.isEmpty() || !data.bible.containsKey(selectedBook)) {
            showBlankReader();
            return;
        }

        currentSourceKey = "BIBLE:" + selectedBook + " " + selectedChapter;
        currentSourceTitle = selectedBook + " " + selectedChapter;

        if (countVisit) {
            currentProfile.visitCounts.put(currentSourceKey, currentProfile.visitCounts.getOrDefault(currentSourceKey, 0) + 1);
            saveData();
        }

        StringBuilder sb = new StringBuilder();
        sb.append(selectedBook).append(" ").append(selectedChapter).append("\n\n");
        for (Verse v : data.getVerses(selectedBook, selectedChapter).values()) {
            sb.append(v.verse).append(" ").append(v.text).append("\n\n");
        }

        setReaderText(sb.toString(), currentSourceKey, currentSourceTitle);
        int visits = currentProfile.visitCounts.getOrDefault(currentSourceKey, 0);
        sourceLabel.setText(currentSourceTitle + " — visited " + visits + " time" + (visits == 1 ? "" : "s"));
    }

    private void showLibraryDoc(String title) {
        LibraryDoc d = data.findLibraryDoc(title);
        if (d == null) return;
        currentSourceKey = "LIBRARY:" + d.title;
        currentSourceTitle = d.title;
        setReaderText(d.title + "\n\n" + d.body, currentSourceKey, currentSourceTitle);
        sourceLabel.setText("Library: " + d.title);
    }

    private void setReaderText(String text, String sourceKey, String sourceTitle) {
        loadingReader = true;
        try {
            StyledDocument doc = readerPane.getStyledDocument();
            doc.remove(0, doc.getLength());
            Style normal = style(doc, "normal", "Georgia", 17, false, new Color(45, 35, 30), null);
            doc.insertString(0, text, normal);
            applyBaseHeadingStyle();
            applyAnnotationsForSource(sourceKey);
            readerPane.setCaretPosition(0);
            showSourceSummary(sourceKey, sourceTitle);
            updateHeader();
        } catch (Exception ex) {
            showError("Reader display failed", ex);
        } finally {
            loadingReader = false;
        }
    }

    private Style style(StyledDocument doc, String name, String family, int size, boolean bold, Color fg, Color bg) {
        Style st = doc.getStyle(name);
        if (st == null) st = doc.addStyle(name, null);
        StyleConstants.setFontFamily(st, family);
        StyleConstants.setFontSize(st, size);
        StyleConstants.setBold(st, bold);
        StyleConstants.setForeground(st, fg);
        if (bg != null) StyleConstants.setBackground(st, bg);
        return st;
    }

    private void applyBaseHeadingStyle() throws BadLocationException {
        StyledDocument doc = readerPane.getStyledDocument();
        String txt = doc.getText(0, doc.getLength());
        int firstBreak = txt.indexOf('\n');
        if (firstBreak >= 0) {
            SimpleAttributeSet heading = new SimpleAttributeSet();
            StyleConstants.setFontFamily(heading, "Segoe UI");
            StyleConstants.setFontSize(heading, 26);
            StyleConstants.setBold(heading, true);
            StyleConstants.setForeground(heading, darkRed);
            doc.setCharacterAttributes(0, firstBreak, heading, false);
        }
    }

    private void applyAnnotationsForSource(String sourceKey) {
        StyledDocument doc = readerPane.getStyledDocument();
        int len = doc.getLength();
        for (TextAnnotation a : currentProfile.annotations) {
            if (!sourceKey.equals(a.sourceKey)) continue;
            if (a.start < 0 || a.end <= a.start || a.start >= len) continue;
            int safeEnd = Math.min(a.end, len);
            SimpleAttributeSet set = new SimpleAttributeSet();
            StyleConstants.setBackground(set, colorForAnnotation(a));
            StyleConstants.setForeground(set, new Color(35, 25, 20));
            StyleConstants.setBold(set, "Question".equals(a.type));
            doc.setCharacterAttributes(a.start, safeEnd - a.start, set, false);
        }
    }

    private Color colorForAnnotation(TextAnnotation a) {
        if ("Category".equals(a.type)) return colorForCategory(a.category);
        if ("Link".equals(a.type)) return linkPurple;
        if ("Question".equals(a.type)) return questionRed;
        if ("Greek".equals(a.type)) return greekGreen;
        return noteYellow;
    }

    private Color colorForCategory(String category) {
        if (category == null || category.trim().isEmpty()) return categoryBlue;
        if (currentProfile != null && currentProfile.categoryColors != null) {
            Integer rgb = currentProfile.categoryColors.get(category);
            if (rgb != null) return new Color(rgb, true);
        }
        return categoryBlue;
    }

    private String colorHex(Color c) {
        if (c == null) c = categoryBlue;
        return String.format("#%02X%02X%02X", c.getRed(), c.getGreen(), c.getBlue());
    }


    private void hideSelectionActionPopup() {
        if (selectionActionPopup != null && selectionActionPopup.isVisible()) {
            selectionActionPopup.setVisible(false);
        }
    }

    private void showSelectionActionPopupIfNeeded() {
        if (loadingReader || currentSourceKey == null || currentSourceKey.isEmpty()) return;
        if (readerPane.getSelectionEnd() <= readerPane.getSelectionStart()) {
            hideSelectionActionPopup();
            return;
        }
        String selected = readerPane.getSelectedText();
        if (selected == null || selected.trim().isEmpty()) {
            hideSelectionActionPopup();
            return;
        }

        hideSelectionActionPopup();
        selectionActionPopup = new JPopupMenu();
        addMenu(selectionActionPopup, "Add Note", () -> addAnnotationFromSelection("Note", ""));
        addMenu(selectionActionPopup, "Add Category", this::addCategoryFromSelection);
        addMenu(selectionActionPopup, "Add Question", () -> addAnnotationFromSelection("Question", ""));
        addMenu(selectionActionPopup, "Attach", this::addAttachmentFromSelection);
        addMenu(selectionActionPopup, "View Greek", this::showGreekForCurrentSelection);

        Point popupPoint = selectionPopupPoint();
        selectionActionPopup.show(readerPane, popupPoint.x, popupPoint.y);
    }

    private Point selectionPopupPoint() {
        try {
            int anchor = Math.min(readerPane.getSelectionStart(), readerPane.getSelectionEnd());
            Rectangle r = readerPane.modelToView2D(anchor).getBounds();
            int x = Math.max(0, r.x);
            int y = Math.max(0, r.y - 34);
            return new Point(x, y);
        } catch (Exception ignored) {
            return new Point(12, 12);
        }
    }

    private void showReaderMenu(MouseEvent e) {
        if (loadingReader || currentSourceKey == null || currentSourceKey.isEmpty()) return;

        int pos = readerPane.viewToModel2D(e.getPoint());
        TextAnnotation existing = annotationAt(pos);
        boolean hasSelection = readerPane.getSelectionEnd() > readerPane.getSelectionStart();

        if (!hasSelection && existing == null) selectWordAt(pos);
        hasSelection = readerPane.getSelectionEnd() > readerPane.getSelectionStart();

        Integer verseNumber = verseNumberAtPosition(pos);
        JPopupMenu menu = new JPopupMenu();

        if (verseNumber != null) {
            String key = selectedBook + " " + selectedChapter + ":" + verseNumber;
            addMenu(menu, "Check Greek For This Verse", () -> showGreekForVerse(key));
            addMenu(menu, "Add Greek Note To This Verse", () -> showGreekForVerse(key));
            menu.addSeparator();
        }

        if (hasSelection) {
            addMenu(menu, "Add Note To Selected Text", () -> addAnnotationFromSelection("Note", ""));
            addMenu(menu, "Add To Category", this::addCategoryFromSelection);
            addMenu(menu, "Attach To Bible Verse Or Book Section", this::addAttachmentFromSelection);
            addMenu(menu, "Add Unfinished Question", () -> addAnnotationFromSelection("Question", ""));
            menu.addSeparator();
        }

        if (existing != null) {
            if ("Category".equals(existing.type) && existing.category != null && !existing.category.trim().isEmpty()) {
                String category = existing.category.trim();
                addMenu(menu, "Show Category: " + category, () -> showCategoryByName(category));
            }
            addMenu(menu, "View Highlight Details", () -> showAnnotationDetails(existing));
            addMenu(menu, "Edit This Highlight", () -> editAnnotation(existing));
            addMenu(menu, "Open Attachment", () -> openAnnotationTarget(existing));
            addMenu(menu, "Delete This Highlight", () -> deleteAnnotation(existing));
        }

        if (menu.getComponentCount() == 0) addMenu(menu, "Select text first", () -> {});
        menu.show(readerPane, e.getX(), e.getY());
    }

    private void addMenu(JPopupMenu m, String label, Runnable r) {
        JMenuItem item = new JMenuItem(label);
        item.addActionListener(e -> r.run());
        m.add(item);
    }

    private void handleReaderLeftClick(MouseEvent e) {
        int pos = readerPane.viewToModel2D(e.getPoint());
        TextAnnotation a = annotationAt(pos);
        if (a != null) showAnnotationDetails(a);
    }

    private void showQuickNoteForSelectionOrWord(MouseEvent e) {
        if (readerPane.getSelectionEnd() <= readerPane.getSelectionStart()) {
            selectWordAt(readerPane.viewToModel2D(e.getPoint()));
        }
        if (readerPane.getSelectionEnd() > readerPane.getSelectionStart()) addAnnotationFromSelection("Note", "");
    }

    private void selectWordAt(int pos) {
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            if (pos < 0 || pos >= text.length()) return;
            int s = pos;
            int e = pos;
            while (s > 0 && Character.isLetterOrDigit(text.charAt(s - 1))) s--;
            while (e < text.length() && Character.isLetterOrDigit(text.charAt(e))) e++;
            if (e > s) readerPane.select(s, e);
        } catch (Exception ignored) {}
    }

    private TextAnnotation annotationAt(int pos) {
        if (currentProfile == null || currentSourceKey == null) return null;
        TextAnnotation best = null;
        for (TextAnnotation a : currentProfile.annotations) {
            if (currentSourceKey.equals(a.sourceKey) && pos >= a.start && pos < a.end) {
                if (best == null || (a.end - a.start) < (best.end - best.start)) best = a;
            }
        }
        return best;
    }

    private Integer verseNumberAtPosition(int pos) {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return null;
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            if (pos < 0 || pos >= text.length()) return null;

            int lineStart = text.lastIndexOf('\n', Math.max(0, pos - 1));
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            int lineEnd = text.indexOf('\n', pos);
            if (lineEnd < 0) lineEnd = text.length();

            int i = lineStart;
            while (i < lineEnd && Character.isWhitespace(text.charAt(i))) i++;
            int numStart = i;
            while (i < lineEnd && Character.isDigit(text.charAt(i))) i++;
            int numEnd = i;

            if (numEnd <= numStart) return null;
            if (pos < numStart || pos > numEnd) return null;
            if (numEnd < lineEnd && !Character.isWhitespace(text.charAt(numEnd))) return null;

            return Integer.parseInt(text.substring(numStart, numEnd));
        } catch (Exception ignored) {
            return null;
        }
    }


    private Integer verseNumberContainingPosition(int pos) {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return null;
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            if (pos < 0 || pos > text.length()) return null;
            if (pos == text.length() && pos > 0) pos--;

            int lineStart = text.lastIndexOf('\n', Math.max(0, pos - 1));
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            int lineEnd = text.indexOf('\n', pos);
            if (lineEnd < 0) lineEnd = text.length();

            int i = lineStart;
            while (i < lineEnd && Character.isWhitespace(text.charAt(i))) i++;
            int numStart = i;
            while (i < lineEnd && Character.isDigit(text.charAt(i))) i++;
            int numEnd = i;

            if (numEnd <= numStart) return null;
            if (numEnd < lineEnd && !Character.isWhitespace(text.charAt(numEnd))) return null;
            if (pos < numStart || pos > lineEnd) return null;

            int verse = Integer.parseInt(text.substring(numStart, numEnd));
            if (!data.getVerses(selectedBook, selectedChapter).containsKey(verse)) return null;
            return verse;
        } catch (Exception ignored) {
            return null;
        }
    }

    private void showGreekForCurrentSelection() {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) {
            JOptionPane.showMessageDialog(this, "Greek lookup is available when selected text is in a Bible chapter.");
            return;
        }
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        if (end <= start || readerPane.getSelectedText() == null || readerPane.getSelectedText().trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Select text in a Bible verse first, then choose View Greek.");
            return;
        }

        Integer verse = verseNumberContainingPosition(start);
        if (verse == null) verse = verseNumberContainingPosition(Math.max(start, end - 1));
        if (verse == null) {
            JOptionPane.showMessageDialog(this, "I could not detect which Bible verse contains that selection.");
            return;
        }

        String key = selectedBook + " " + selectedChapter + ":" + verse;
        showGreekForVerse(key);
    }

    private int[] verseRangeInReader(int verse) {
        try {
            String txt = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            String marker = "\n" + verse + " ";
            int idx = txt.indexOf(marker);
            if (idx < 0 && txt.startsWith(verse + " ")) idx = 0;
            if (idx < 0) return new int[]{0, 0};
            int start = idx == 0 ? 0 : idx + 1;
            int end = txt.indexOf("\n\n", start);
            if (end < 0) end = txt.length();
            return new int[]{start, end};
        } catch (Exception ignored) {
            return new int[]{0, 0};
        }
    }

    private void showGreekForVerse(String key) {
        GreekEntry ge = data.greek.get(key);

        JTextArea info = new JTextArea(12, 58);
        info.setFont(new Font("Consolas", Font.PLAIN, 13));
        info.setLineWrap(true);
        info.setWrapStyleWord(true);
        info.setEditable(false);

        if (ge == null) {
            info.setText("No Greek imported for " + key + " yet.\n\nUse Import > Download + Import MorphGNT Greek, or import a MorphGNT ZIP/TXT folder, or import a Greek CSV.");
        } else {
            info.setText("Greek Text:\n" + ge.greekText + "\n\nDetails:\n" + ge.details);
        }

        JTextArea note = new JTextArea(5, 58);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel p = new JPanel(new BorderLayout(6, 6));
        p.add(new JScrollPane(info), BorderLayout.CENTER);
        p.add(new JLabel("Optional Greek note to save on this verse:"), BorderLayout.NORTH);
        p.add(new JScrollPane(note), BorderLayout.SOUTH);

        int r = JOptionPane.showConfirmDialog(this, p, "Greek Lookup - " + key, JOptionPane.OK_CANCEL_OPTION);
        if (r == JOptionPane.OK_OPTION && !note.getText().trim().isEmpty()) {
            RefParts rp = parseRef(key);
            int[] range = rp == null ? new int[]{0, 0} : verseRangeInReader(rp.verse);
            String selected = key;
            try {
                if (range[1] > range[0]) selected = readerPane.getDocument().getText(range[0], range[1] - range[0]);
            } catch (Exception ignored) {}

            TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, range[0], range[1], selected, "Greek", "", note.getText().trim(), key);
            currentProfile.annotations.add(a);
            saveData();
            reloadCurrentSource();
            showAnnotationDetails(a);
        }
    }

    private void addAnnotationFromSelection(String type, String category) {
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        if (end <= start) {
            JOptionPane.showMessageDialog(this, "Highlight/select text first, then right-click it.");
            return;
        }

        String selected = readerPane.getSelectedText();
        JTextArea note = new JTextArea(8, 44);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        int r = JOptionPane.showConfirmDialog(this, new JScrollPane(note), "Add " + type + " to: " + shorten(selected, 70), JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        String body = note.getText().trim();
        if (body.isEmpty() && !"Category".equals(type)) return;

        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, start, end, selected, type, category, body, "");
        currentProfile.annotations.add(a);

        if ("Question".equals(type)) {
            currentProfile.questions.add(new StudyQuestion(a.id, currentSourceTitle, selected, body));
        }

        saveData();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void addCategoryFromSelection() {
        String cat = chooseOrCreateCategory();
        if (cat == null || cat.trim().isEmpty()) return;

        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        if (end <= start) return;

        String selected = readerPane.getSelectedText();
        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, start, end, selected, "Category", cat, "Added to category: " + cat, "");
        currentProfile.annotations.add(a);

        saveData();
        refreshCategories();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private String chooseOrCreateCategory() {
        ensureCategoryColors();

        Object[] opts = new Object[currentProfile.categories.size() + 1];
        opts[0] = "+ Create New Category";
        int i = 1;
        for (String c : currentProfile.categories.keySet()) opts[i++] = c;

        Object o = JOptionPane.showInputDialog(this, "Choose category:", "Category", JOptionPane.PLAIN_MESSAGE, null, opts, opts[0]);
        if (o == null) return null;

        String cat = o.toString();
        if (cat.startsWith("+")) return createCategory(null);
        return cat;
    }

    private void addAttachmentFromSelection() {
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        if (end <= start) return;
        String selected = readerPane.getSelectedText();

        JTextField bibleRef = new JTextField();
        JComboBox<String> docBox = new JComboBox<>();
        docBox.addItem("");
        for (LibraryDoc d : data.libraryDocs) docBox.addItem(d.title);

        JTextArea chunk = new JTextArea(4, 42);
        chunk.setLineWrap(true);
        chunk.setWrapStyleWord(true);

        JTextField relation = new JTextField("Cross-reference");
        JTextArea note = new JTextArea(5, 42);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Attach to Bible ref, e.g. Romans 14:13. Leave blank if attaching to a book chunk."));
        p.add(bibleRef);
        p.add(new JLabel("OR attach to a selected imported book:"));
        p.add(docBox);
        p.add(new JLabel("Exact chunk from that book. The app will accept it only if it appears exactly once:"));
        p.add(new JScrollPane(chunk));
        p.add(new JLabel("Relationship label:"));
        p.add(relation);
        p.add(new JLabel("Note:"));
        p.add(new JScrollPane(note));

        int r = JOptionPane.showConfirmDialog(this, p, "Attach Selected Text", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        String target;
        String ref = bibleRef.getText().trim();
        if (!ref.isEmpty()) {
            RefParts rp = parseRef(ref);
            if (rp == null || data.findVerse(rp.key()) == null) {
                JOptionPane.showMessageDialog(this, "I could not find that Bible verse. Use a format like Romans 14:13.");
                return;
            }
            target = rp.key();
        } else {
            String docTitle = Objects.toString(docBox.getSelectedItem(), "").trim();
            String chunkText = chunk.getText().trim();
            if (docTitle.isEmpty() || chunkText.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Enter a Bible ref or choose a book and enter a unique chunk.");
                return;
            }
            LibraryDoc d = data.findLibraryDoc(docTitle);
            if (d == null) return;
            int first = normalizeForFind(d.body).indexOf(normalizeForFind(chunkText));
            if (first < 0) {
                JOptionPane.showMessageDialog(this, "That chunk was not found in the selected book.");
                return;
            }
            int second = normalizeForFind(d.body).indexOf(normalizeForFind(chunkText), first + 1);
            if (second >= 0) {
                JOptionPane.showMessageDialog(this, "That chunk appears more than once. Add more words until it only appears once.");
                return;
            }
            target = "LIBRARY:" + docTitle + "::" + chunkText;
        }

        String body = "Relationship: " + relation.getText().trim() + "\n" + note.getText().trim();
        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, start, end, selected, "Link", "", body, target);
        currentProfile.annotations.add(a);
        saveData();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void editAnnotation(TextAnnotation a) {
        JTextField type = new JTextField(a.type);
        JTextField cat = new JTextField(a.category);
        JTextField target = new JTextField(a.target);
        JTextArea note = new JTextArea(a.note, 8, 44);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Type:")); p.add(type);
        p.add(new JLabel("Category:")); p.add(cat);
        p.add(new JLabel("Target / attachment:")); p.add(target);
        p.add(new JLabel("Note:")); p.add(new JScrollPane(note));

        int r = JOptionPane.showConfirmDialog(this, p, "Edit Highlight", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        a.type = type.getText().trim().isEmpty() ? "Note" : type.getText().trim();
        a.category = cat.getText().trim();
        a.target = target.getText().trim();
        a.note = note.getText().trim();

        saveData();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void deleteAnnotation(TextAnnotation a) {
        if (JOptionPane.showConfirmDialog(this, "Delete this highlight?", "Delete", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        currentProfile.annotations.removeIf(x -> x.id.equals(a.id));
        currentProfile.questions.removeIf(q -> q.annotationId.equals(a.id));
        saveData();
        reloadCurrentSource();
        showSourceSummary(currentSourceKey, currentSourceTitle);
    }

    private void reloadCurrentSource() {
        if (currentSourceKey == null) return;
        if (currentSourceKey.startsWith("BIBLE:")) showSelectedChapter(false);
        else if (currentSourceKey.startsWith("LIBRARY:")) showLibraryDoc(currentSourceKey.substring("LIBRARY:".length()));
    }

    private void showAnnotationDetails(TextAnnotation a) {
        detailsPanel.removeAll();
        addDetailTitle(a.type + " Highlight");
        addDetailText("Source: " + a.sourceTitle + "\nSelected text: “" + a.selectedText + "”");
        if (!a.category.isEmpty()) addDetailText("Category: " + a.category + "\nColor: " + colorHex(colorForCategory(a.category)));
        if (!a.target.isEmpty()) {
            addDetailText("Attached to: " + a.target);
            JButton open = blackButton("Open Attachment");
            open.setAlignmentX(Component.LEFT_ALIGNMENT);
            open.addActionListener(e -> openAnnotationTarget(a));
            detailsPanel.add(open);
            detailsPanel.add(Box.createVerticalStrut(8));
        }
        if (!a.note.isEmpty()) addDetailText(a.note);

        JButton edit = blackButton("Edit Highlight");
        edit.setAlignmentX(Component.LEFT_ALIGNMENT);
        edit.addActionListener(e -> editAnnotation(a));

        JButton del = blackButton("Delete Highlight");
        del.setAlignmentX(Component.LEFT_ALIGNMENT);
        del.addActionListener(e -> deleteAnnotation(a));

        detailsPanel.add(edit);
        detailsPanel.add(Box.createVerticalStrut(6));
        detailsPanel.add(del);
        detailsPanel.revalidate();
        detailsPanel.repaint();
    }

    private void openAnnotationTarget(TextAnnotation a) {
        if (a == null || a.target == null || a.target.trim().isEmpty()) return;
        openTarget(a.target.trim());
    }

    private void openTarget(String target) {
        if (target == null || target.trim().isEmpty()) return;
        target = target.trim();

        if (target.startsWith("LIBRARY:")) {
            String rest = target.substring("LIBRARY:".length());
            String title = rest;
            String chunk = "";
            int split = rest.indexOf("::");
            if (split >= 0) {
                title = rest.substring(0, split);
                chunk = rest.substring(split + 2);
            }
            showLibraryDoc(title);
            if (!chunk.isEmpty()) findAndSelectUniqueChunk(chunk);
            showCard("study");
            return;
        }

        RefParts rp = parseRef(target);
        if (rp != null && data.bible.containsKey(rp.book)) {
            selectedBook = rp.book;
            selectedChapter = rp.chapter;
            refreshBookCombo();
            showSelectedChapter(false);
            selectVerseText(rp.verse);
            showCard("study");
        }
    }

    private void findAndSelectUniqueChunk(String chunk) {
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            String nText = normalizeForFind(text);
            String nChunk = normalizeForFind(chunk);
            int idx = nText.indexOf(nChunk);
            if (idx < 0) return;
            int second = nText.indexOf(nChunk, idx + 1);
            if (second >= 0) return;

            int[] real = normalizedIndexToRealRange(text, chunk, idx);
            readerPane.requestFocusInWindow();
            readerPane.select(real[0], real[1]);
            readerPane.setCaretPosition(real[0]);
        } catch (Exception ignored) {}
    }

    private int[] normalizedIndexToRealRange(String full, String chunk, int normalizedStart) {
        StringBuilder norm = new StringBuilder();
        int realStart = -1;
        int realEnd = full.length();
        int targetLen = normalizeForFind(chunk).length();

        for (int i = 0; i < full.length(); i++) {
            char c = full.charAt(i);
            boolean add = !Character.isWhitespace(c) || (norm.length() > 0 && norm.charAt(norm.length() - 1) != ' ');
            if (add) {
                if (norm.length() == normalizedStart) realStart = i;
                norm.append(Character.isWhitespace(c) ? ' ' : Character.toLowerCase(c));
                if (realStart >= 0 && norm.length() >= normalizedStart + targetLen) {
                    realEnd = i + 1;
                    break;
                }
            }
        }
        if (realStart < 0) realStart = 0;
        return new int[]{realStart, realEnd};
    }

    private void selectVerseText(int verse) {
        try {
            String txt = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            String marker = "\n" + verse + " ";
            int idx = txt.indexOf(marker);
            if (idx < 0 && txt.startsWith(verse + " ")) idx = 0;
            if (idx >= 0) {
                int start = idx + (idx == 0 ? 0 : 1);
                int end = txt.indexOf("\n\n", start);
                if (end < 0) end = txt.length();
                readerPane.requestFocusInWindow();
                readerPane.select(start, end);
                readerPane.setCaretPosition(start);
            }
        } catch (Exception ignored) {}
    }

    private void showSourceSummary(String sourceKey, String sourceTitle) {
        detailsPanel.removeAll();
        addDetailTitle(sourceTitle == null || sourceTitle.isEmpty() ? "Current Source" : sourceTitle);
        int count = 0;
        for (TextAnnotation a : currentProfile.annotations) if (sourceKey.equals(a.sourceKey)) count++;
        addDetailText(count + " highlight note(s) in this source.\n\nSelect text and right-click to add a note, category, attachment, or question. Hover over highlighted text to preview notes. Click a highlight to view actions here.");
        detailsPanel.revalidate();
        detailsPanel.repaint();
    }

    private void showDetailsText(String text) {
        detailsPanel.removeAll();
        addDetailText(text);
        detailsPanel.revalidate();
        detailsPanel.repaint();
    }

    private void addDetailTitle(String s) {
        JLabel l = new JLabel("<html><b>" + esc(s) + "</b></html>");
        l.setFont(new Font("Segoe UI", Font.BOLD, 18));
        l.setForeground(darkRed);
        l.setAlignmentX(Component.LEFT_ALIGNMENT);
        detailsPanel.add(l);
        detailsPanel.add(Box.createVerticalStrut(8));
    }

    private void addDetailText(String s) {
        JTextArea a = readonlyArea();
        a.setText(s == null ? "" : s);
        a.setAlignmentX(Component.LEFT_ALIGNMENT);
        detailsPanel.add(a);
        detailsPanel.add(Box.createVerticalStrut(8));
    }

    private String createCategory(String optionalAnnotationId) {
        JTextField name = new JTextField();
        JTextArea desc = new JTextArea(5, 35);
        desc.setLineWrap(true);
        desc.setWrapStyleWord(true);

        JButton chooseColor = blackButton("Choose Highlight Color");
        JLabel colorPreview = new JLabel("Default blue " + colorHex(categoryBlue));
        colorPreview.setOpaque(true);
        colorPreview.setBackground(categoryBlue);
        colorPreview.setBorder(new CompoundBorder(new LineBorder(Color.DARK_GRAY), new EmptyBorder(6, 8, 6, 8)));

        final Color[] selectedColor = new Color[]{categoryBlue};
        chooseColor.addActionListener(e -> {
            Color c = JColorChooser.showDialog(this, "Choose Category Highlight Color", selectedColor[0]);
            if (c != null) {
                selectedColor[0] = c;
                colorPreview.setBackground(c);
                colorPreview.setText(colorHex(c));
            }
        });

        JPanel colorPanel = new JPanel(new BorderLayout(6, 6));
        colorPanel.add(chooseColor, BorderLayout.WEST);
        colorPanel.add(colorPreview, BorderLayout.CENTER);

        JPanel p = new JPanel(new BorderLayout(6, 6));
        p.add(new JLabel("Category name:"), BorderLayout.NORTH);
        p.add(name, BorderLayout.CENTER);

        JPanel bottom = new JPanel(new BorderLayout(6, 6));
        bottom.add(new JScrollPane(desc), BorderLayout.CENTER);
        bottom.add(colorPanel, BorderLayout.SOUTH);
        p.add(bottom, BorderLayout.SOUTH);

        int r = JOptionPane.showConfirmDialog(this, p, "Create Category", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return null;

        String n = name.getText().trim();
        if (n.isEmpty()) return null;

        currentProfile.categories.put(n, desc.getText().trim());
        ensureCategoryColors();
        currentProfile.categoryColors.put(n, selectedColor[0].getRGB());

        saveData();
        refreshCategories();
        reloadCurrentSource();
        return n;
    }

    private void refreshCategories() {
        if (categoryModel == null) return;
        ensureCategoryColors();
        categoryModel.clear();

        for (String c : currentProfile.categories.keySet()) {
            currentProfile.categoryColors.putIfAbsent(c, categoryBlue.getRGB());
            int count = 0;
            for (TextAnnotation a : currentProfile.annotations) if (c.equals(a.category)) count++;
            categoryModel.addElement(c + " (" + count + ") " + colorHex(colorForCategory(c)));
        }
    }

    private void ensureCategoryColors() {
        if (currentProfile.categoryColors == null) currentProfile.categoryColors = new TreeMap<>();
        if (currentProfile.categories == null) currentProfile.categories = new TreeMap<>();
        for (String c : currentProfile.categories.keySet()) currentProfile.categoryColors.putIfAbsent(c, categoryBlue.getRGB());
    }

    private void changeSelectedCategoryColor() {
        String s = categoryList.getSelectedValue();
        if (s == null) {
            JOptionPane.showMessageDialog(this, "Select a category first.");
            return;
        }

        String cat = selectedCategoryNameFromListValue(s);
        if (cat.isEmpty()) return;

        Color current = colorForCategory(cat);
        Color chosen = JColorChooser.showDialog(this, "Choose Highlight Color for " + cat, current);
        if (chosen == null) return;

        ensureCategoryColors();
        currentProfile.categoryColors.put(cat, chosen.getRGB());
        saveData();
        refreshCategories();
        reloadCurrentSource();
        JOptionPane.showMessageDialog(this, "Updated highlight color for: " + cat);
    }

    private String selectedCategoryNameFromListValue(String s) {
        if (s == null) return "";
        return s.replaceAll("\\s+\\(.*$", "").trim();
    }

    private void viewSelectedCategory() {
        String s = categoryList.getSelectedValue();
        if (s == null) return;

        showCategoryByName(selectedCategoryNameFromListValue(s));
    }

    private void showCategoryByName(String cat) {
        if (cat == null || cat.trim().isEmpty()) return;
        cat = cat.trim();

        refreshCategories();
        for (int i = 0; i < categoryModel.size(); i++) {
            if (cat.equals(selectedCategoryNameFromListValue(categoryModel.getElementAt(i)))) {
                categoryList.setSelectedIndex(i);
                categoryList.ensureIndexIsVisible(i);
                break;
            }
        }

        showCategoryDetails(cat);
        showCard("study");
    }

    private void showCategoryDetails(String cat) {
        detailsPanel.removeAll();
        addDetailTitle("Category: " + cat);
        addDetailText(currentProfile.categories.getOrDefault(cat, "") + "\nHighlight color: " + colorHex(colorForCategory(cat)));

        for (TextAnnotation a : currentProfile.annotations) {
            if (cat.equals(a.category)) {
                JButton b = blackButton(a.sourceTitle + " — “" + shorten(a.selectedText, 70) + "”");
                b.setAlignmentX(Component.LEFT_ALIGNMENT);
                b.addActionListener(e -> {
                    openSourceForAnnotation(a);
                    safeSelect(a.start, a.end);
                    showAnnotationDetails(a);
                });
                detailsPanel.add(b);
                detailsPanel.add(Box.createVerticalStrut(6));
            }
        }

        detailsPanel.revalidate();
        detailsPanel.repaint();
    }

    private void openSourceForAnnotation(TextAnnotation a) {
        if (a.sourceKey.startsWith("BIBLE:")) {
            String ref = a.sourceKey.substring("BIBLE:".length()) + ":1";
            RefParts rp = parseRef(ref);
            if (rp != null) {
                selectedBook = rp.book;
                selectedChapter = rp.chapter;
                refreshBookCombo();
                showSelectedChapter(false);
            }
        } else if (a.sourceKey.startsWith("LIBRARY:")) {
            showLibraryDoc(a.sourceKey.substring("LIBRARY:".length()));
        }
    }

    private void addQuestionForSelection() {
        addAnnotationFromSelection("Question", "");
    }

    private void refreshQuestions() {
        if (questionModel == null) return;
        questionModel.clear();
        for (int i = 0; i < currentProfile.questions.size(); i++) {
            StudyQuestion q = currentProfile.questions.get(i);
            questionModel.addElement(i + " | " + (q.answered ? "✓" : "❗") + " | " + q.sourceTitle + " | " + shorten(q.question, 140));
        }
        updateHeader();
    }

    private int countUnanswered() {
        int c = 0;
        for (StudyQuestion q : currentProfile.questions) if (!q.answered) c++;
        return c;
    }

    private void toggleSelectedQuestion() {
        String s = questionList.getSelectedValue();
        if (s == null) return;
        try {
            int idx = Integer.parseInt(s.split("\\|")[0].trim());
            StudyQuestion q = currentProfile.questions.get(idx);
            q.answered = !q.answered;
            saveData();
            refreshQuestions();
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Could not toggle question.");
        }
    }

    private void toggleSideSearch() {
        sideSearchExpanded = !sideSearchExpanded;
        if (sideSearchBody != null) sideSearchBody.setVisible(sideSearchExpanded);
        if (sideSearchToggleBtn != null) sideSearchToggleBtn.setText(sideSearchExpanded ? "Minimize" : "Restore");
    }

    private void doSideSearch() {
        if (sideSearchModel == null) return;
        sideSearchModel.clear();
        String q = sideSearchField == null ? "" : sideSearchField.getText().trim().toLowerCase(Locale.ROOT);
        if (q.isEmpty()) {
            sideSearchPreview.setText("Type a search above.");
            return;
        }
        fillSearchModel(sideSearchModel, q, 120);
        sideSearchPreview.setText(sideSearchModel.isEmpty() ? "No results found." : sideSearchModel.size() + " result(s). Click to preview. Double-click or right-click > Show Full View to open.");
    }

    private void previewSideSearchResult() {
        String s = sideSearchList.getSelectedValue();
        if (s == null) return;
        SearchResultParts p = parseSearchLine(s);
        sideSearchPreview.setText(previewForSearchResult(p));
        sideSearchPreview.setCaretPosition(0);
    }

    private void showSideSearchMenu(MouseEvent e) {
        if (sideSearchList == null) return;
        int idx = sideSearchList.locationToIndex(e.getPoint());
        if (idx >= 0) sideSearchList.setSelectedIndex(idx);
        if (sideSearchList.getSelectedValue() == null) return;

        JPopupMenu m = new JPopupMenu();
        addMenu(m, "View In Sidebar", this::previewSideSearchResult);
        addMenu(m, "Show Full View", this::showFullViewForSideSearchResult);
        m.show(sideSearchList, e.getX(), e.getY());
    }

    private void showFullViewForSideSearchResult() {
        String s = sideSearchList.getSelectedValue();
        if (s == null) return;
        openSearchLineFullView(s);
    }

    private void doSearch() {
        if (searchModel == null) return;
        searchModel.clear();
        String q = searchField.getText().trim().toLowerCase(Locale.ROOT);
        if (q.isEmpty()) return;
        fillSearchModel(searchModel, q, 150);
    }

    private void fillSearchModel(DefaultListModel<String> model, String q, int maxSnippet) {
        for (String book : data.bible.keySet()) {
            for (Integer ch : data.getChapters(book)) {
                for (Verse v : data.getVerses(book, ch).values()) {
                    if ((v.key() + " " + v.text).toLowerCase(Locale.ROOT).contains(q)) {
                        model.addElement("BIBLE | " + v.key() + " | " + shorten(v.text, maxSnippet));
                    }
                }
            }
        }

        for (LibraryDoc d : data.libraryDocs) {
            if ((d.title + " " + d.body).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("LIBRARY | " + d.title + " | " + shorten(snippet(d.body, q), maxSnippet));
            }
        }

        for (GreekEntry ge : data.greek.values()) {
            if ((ge.key() + " " + ge.greekText + " " + ge.details).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("GREEK | " + ge.key() + " | " + shorten(ge.greekText + " — " + ge.details, maxSnippet));
            }
        }

        for (TextAnnotation a : currentProfile.annotations) {
            if ((a.sourceTitle + " " + a.selectedText + " " + a.type + " " + a.category + " " + a.note + " " + a.target).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("HIGHLIGHT | " + a.id + " | " + a.sourceTitle + " | " + shorten(a.selectedText + " — " + a.note, maxSnippet));
            }
        }

        for (StudyQuestion qu : currentProfile.questions) {
            if ((qu.sourceTitle + " " + qu.selectedText + " " + qu.question).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("QUESTION | " + qu.annotationId + " | " + qu.sourceTitle + " | " + shorten(qu.question, maxSnippet));
            }
        }
    }

    private void openSearchResult() {
        String s = searchList.getSelectedValue();
        if (s == null) return;
        openSearchLineFullView(s);
    }

    private SearchResultParts parseSearchLine(String s) {
        String[] p = s.split("\\|", 4);
        return new SearchResultParts(
                p.length > 0 ? p[0].trim() : "",
                p.length > 1 ? p[1].trim() : "",
                p.length > 2 ? p[2].trim() : "",
                p.length > 3 ? p[3].trim() : ""
        );
    }

    private String previewForSearchResult(SearchResultParts p) {
        StringBuilder sb = new StringBuilder();
        sb.append(p.type).append("\n----------------------\n");

        if (p.type.equals("BIBLE")) {
            Verse v = data.findVerse(p.ref);
            sb.append(p.ref).append("\n\n");
            sb.append(v == null ? p.extra : v.text);
        } else if (p.type.equals("GREEK")) {
            GreekEntry ge = data.greek.get(p.ref);
            sb.append(p.ref).append("\n\n");
            if (ge != null) {
                sb.append("Greek Text:\n").append(ge.greekText).append("\n\nDetails:\n").append(ge.details);
            } else {
                sb.append(p.extra);
            }
        } else if (p.type.equals("LIBRARY")) {
            sb.append(p.ref).append("\n\n").append(p.extra);
        } else if (p.type.equals("HIGHLIGHT") || p.type.equals("QUESTION")) {
            TextAnnotation a = findAnnotationById(p.ref);
            if (a != null) {
                sb.append(a.sourceTitle).append("\n\nSelected Text:\n").append(a.selectedText).append("\n\n");
                if (!a.category.isEmpty()) sb.append("Category: ").append(a.category).append("\n\n");
                if (!a.target.isEmpty()) sb.append("Attached To: ").append(a.target).append("\n\n");
                sb.append(a.note);
            } else {
                sb.append(p.title).append("\n\n").append(p.extra);
            }
        } else {
            sb.append(p.extra);
        }

        return sb.toString();
    }

    private void openSearchLineFullView(String s) {
        SearchResultParts p = parseSearchLine(s);
        if (p.type.equals("BIBLE") || p.type.equals("GREEK")) {
            openTarget(p.ref);
            return;
        }
        if (p.type.equals("LIBRARY")) {
            showLibraryDoc(p.ref);
            showCard("study");
            return;
        }
        if (p.type.equals("HIGHLIGHT") || p.type.equals("QUESTION")) {
            TextAnnotation a = findAnnotationById(p.ref);
            if (a != null) {
                openSourceForAnnotation(a);
                safeSelect(a.start, a.end);
                showAnnotationDetails(a);
                showCard("study");
            }
        }
    }

    private TextAnnotation findAnnotationById(String id) {
        if (id == null || currentProfile == null) return null;
        for (TextAnnotation a : currentProfile.annotations) if (id.equals(a.id)) return a;
        return null;
    }

    private void safeSelect(int start, int end) {
        int len = readerPane.getDocument().getLength();
        int s = Math.max(0, Math.min(start, len));
        int e = Math.max(s, Math.min(end, len));
        readerPane.requestFocusInWindow();
        readerPane.select(s, e);
        readerPane.setCaretPosition(s);
    }

    private RefParts parseRef(String key) {
        try {
            key = key.trim().replaceAll("\\s+", " ");
            int colon = key.lastIndexOf(':');
            if (colon < 0) return null;
            String left = key.substring(0, colon).trim();
            int verse = Integer.parseInt(key.substring(colon + 1).trim());
            int sp = left.lastIndexOf(' ');
            if (sp < 0) return null;
            String book = normalizeBookName(left.substring(0, sp).trim());
            int chapter = Integer.parseInt(left.substring(sp + 1).trim());
            return new RefParts(book, chapter, verse);
        } catch (Exception e) {
            return null;
        }
    }

    private String normalizeBookName(String s) {
        if (s == null) return "";
        s = s.trim().replaceAll("\\s+", " ");
        for (String b : canonicalBooks()) if (b.equalsIgnoreCase(s)) return b;
        return s;
    }

    private List<String> canonicalBooks() {
        List<String> out = new ArrayList<>(bibleBookOrder().keySet());
        List<String> result = new ArrayList<>();
        for (String lower : out) {
            StringBuilder sb = new StringBuilder();
            for (String part : lower.split(" ")) {
                if (sb.length() > 0) sb.append(' ');
                if (part.length() == 1 && Character.isDigit(part.charAt(0))) sb.append(part);
                else sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
            result.add(sb.toString());
        }
        return result;
    }

    private String snippet(String body, String q) {
        String l = body.toLowerCase(Locale.ROOT);
        int i = l.indexOf(q);
        if (i < 0) return shorten(body, 180);
        int st = Math.max(0, i - 70);
        int en = Math.min(body.length(), i + 140);
        return body.substring(st, en).replace("\n", " ");
    }

    private String shorten(String s, int max) {
        if (s == null) return "";
        s = s.replace("\r", " ").replace("\n", " ").trim();
        return s.length() <= max ? s : s.substring(0, Math.max(0, max)) + "...";
    }

    private String esc(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private String normalizeForFind(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    }

    private void downloadAndImportBsbUsfm() {
        new Thread(() -> {
            try {
                log("Downloading official BSB USFM ZIP...");
                byte[] bytes = download(OFFICIAL_BSB_USFM_ZIP);
                log("Downloaded " + bytes.length + " bytes. Importing...");
                int count = importUsfmZipBytes(bytes);
                log("Imported " + count + " BSB verses. Saved permanently.");
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("Download/import BSB failed", ex);
            }
        }).start();
    }

    private void downloadAndImportMorphGnt() {
        new Thread(() -> {
            try {
                log("Downloading MorphGNT/SBLGNT ZIP...");
                byte[] bytes = download(MORPHGNT_ZIP);
                log("Downloaded " + bytes.length + " bytes. Importing...");
                int count = importMorphGntZipBytes(bytes);
                log("Imported " + count + " Greek verse entries. Saved permanently.");
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("Download/import Greek failed", ex);
            }
        }).start();
    }

    private byte[] download(String url) throws IOException {
        URLConnection c = new URL(url).openConnection();
        c.setRequestProperty("User-Agent", "BibleReaderApp/3.0");
        c.setConnectTimeout(20000);
        c.setReadTimeout(60000);
        try (InputStream in = c.getInputStream(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            return out.toByteArray();
        }
    }

    private void importUsfmZipOrFolder() {
        JFileChooser ch = new JFileChooser(new File("."));
        ch.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File f = ch.getSelectedFile();
        new Thread(() -> {
            try {
                int count = f.isDirectory() ? importUsfmFolder(f) : importUsfmZipBytes(Files.readAllBytes(f.toPath()));
                log("Imported " + count + " verses from USFM.");
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("USFM import failed", ex);
            }
        }).start();
    }

    private int importUsfmFolder(File dir) throws IOException {
        int count = 0;
        File[] files = dir.listFiles();
        if (files == null) return 0;
        for (File f : files) {
            if (f.isDirectory()) count += importUsfmFolder(f);
            else if (isTextLike(f.getName())) count += parseUsfm(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8));
        }
        saveData();
        return count;
    }

    private boolean isTextLike(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        return n.endsWith(".usfm") || n.endsWith(".sfm") || n.endsWith(".txt");
    }

    private int importUsfmZipBytes(byte[] bytes) throws IOException {
        int count = 0;
        try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                if (!e.isDirectory() && isTextLike(e.getName())) count += parseUsfm(new String(readAll(zin), StandardCharsets.UTF_8));
            }
        }
        saveData();
        return count;
    }

    private byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
        return out.toByteArray();
    }

    private int parseUsfm(String txt) {
        int imported = 0;
        String book = "";
        int chapter = 0;
        int pendingVerse = -1;
        StringBuilder pending = new StringBuilder();
        txt = txt.replace("\uFEFF", "");

        for (String raw : txt.split("\\R")) {
            String line = raw.trim();
            if (line.isEmpty()) continue;

            if (hasUsfmMarker(line, "id")) {
                String rest = removeLeadingUsfmMarker(line, "id");
                String[] parts = rest.split("\\s+", 2);
                if (parts.length > 0 && book.isEmpty()) book = bookCodeToName(parts[0].replaceAll("[^A-Za-z0-9]", ""));
                continue;
            }

            if (hasUsfmMarker(line, "toc1")) { book = normalizeBookName(removeLeadingUsfmMarker(line, "toc1")); continue; }
            if (hasUsfmMarker(line, "h") && book.isEmpty()) { book = normalizeBookName(removeLeadingUsfmMarker(line, "h")); continue; }

            if (hasUsfmMarker(line, "c")) {
                if (pendingVerse > 0 && !book.isEmpty() && chapter > 0) {
                    String cleaned = cleanUsfmText(pending.toString());
                    if (!cleaned.isEmpty()) { data.putVerse(new Verse(book, chapter, pendingVerse, cleaned)); imported++; }
                }
                pending.setLength(0);
                pendingVerse = -1;
                String rest = removeLeadingUsfmMarker(line, "c");
                String[] parts = rest.split("\\s+", 2);
                try { chapter = Integer.parseInt(parts[0].replaceAll("[^0-9]", "")); } catch (Exception ignored) {}
                if (parts.length > 1) line = parts[1].trim(); else continue;
            }

            List<VerseMarker> markers = findVerseMarkers(line);
            if (!markers.isEmpty()) {
                for (int i = 0; i < markers.size(); i++) {
                    VerseMarker current = markers.get(i);
                    if (pendingVerse > 0) {
                        String cleaned = cleanUsfmText(pending.toString());
                        if (!book.isEmpty() && chapter > 0 && !cleaned.isEmpty()) { data.putVerse(new Verse(book, chapter, pendingVerse, cleaned)); imported++; }
                        pending.setLength(0);
                    }
                    pendingVerse = current.verse;
                    int textStart = current.end;
                    int textEnd = (i + 1 < markers.size()) ? markers.get(i + 1).start : line.length();
                    if (textStart >= 0 && textStart <= textEnd && textEnd <= line.length()) pending.append(line, textStart, textEnd).append(' ');
                }
            } else if (pendingVerse > 0) {
                pending.append(line).append(' ');
            }
        }

        if (pendingVerse > 0 && !book.isEmpty() && chapter > 0) {
            String cleaned = cleanUsfmText(pending.toString());
            if (!cleaned.isEmpty()) { data.putVerse(new Verse(book, chapter, pendingVerse, cleaned)); imported++; }
        }

        return imported;
    }

    private boolean hasUsfmMarker(String line, String marker) {
        String trimmed = line == null ? "" : line.trim();
        String prefix = "\\" + marker;
        if (!trimmed.startsWith(prefix)) return false;
        return trimmed.length() == prefix.length() || Character.isWhitespace(trimmed.charAt(prefix.length()));
    }

    private String removeLeadingUsfmMarker(String line, String marker) {
        String trimmed = line == null ? "" : line.trim();
        String prefix = "\\" + marker;
        return trimmed.startsWith(prefix) ? trimmed.substring(prefix.length()).trim() : trimmed;
    }

    private List<VerseMarker> findVerseMarkers(String line) {
        List<VerseMarker> markers = new ArrayList<>();
        int i = 0;
        while (i < line.length()) {
            int idx = line.indexOf("\\v", i);
            if (idx < 0) break;
            int cursor = idx + 2;
            while (cursor < line.length() && Character.isWhitespace(line.charAt(cursor))) cursor++;
            int numberStart = cursor;
            while (cursor < line.length() && Character.isDigit(line.charAt(cursor))) cursor++;
            if (numberStart == cursor) { i = idx + 2; continue; }
            String numText = line.substring(numberStart, cursor);
            while (cursor < line.length() && (line.charAt(cursor) == '-' || line.charAt(cursor) == '–' || Character.isDigit(line.charAt(cursor)))) cursor++;
            while (cursor < line.length() && Character.isWhitespace(line.charAt(cursor))) cursor++;
            try { markers.add(new VerseMarker(idx, cursor, Integer.parseInt(numText))); } catch (Exception ignored) {}
            i = cursor;
        }
        return markers;
    }

    private String cleanUsfmText(String s) {
        if (s == null) return "";
        String cleaned = removeUsfmSpan(removeUsfmSpan(s, "\\f", "\\f*"), "\\x", "\\x*");
        cleaned = cleanUsfmWordAttributes(cleaned);
        cleaned = cleaned.replaceAll("\\\\[a-zA-Z0-9]+\\*", " ");
        cleaned = cleaned.replaceAll("\\\\[a-zA-Z]+[0-9]*\\s*", " ");
        cleaned = cleaned.replaceAll("\\|[^\\s]+", " ");
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned;
    }

    private String removeUsfmSpan(String text, String startMarker, String endMarker) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            int start = text.indexOf(startMarker, i);
            if (start < 0) { out.append(text.substring(i)); break; }
            out.append(text, i, start);
            int end = text.indexOf(endMarker, start + startMarker.length());
            if (end < 0) break;
            i = end + endMarker.length();
        }
        return out.toString();
    }

    private String cleanUsfmWordAttributes(String text) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            int markerStart = text.indexOf("\\w ", i);
            if (markerStart < 0) { out.append(text.substring(i)); break; }
            out.append(text, i, markerStart);
            int contentStart = markerStart + 3;
            int markerEnd = text.indexOf("\\w*", contentStart);
            if (markerEnd < 0) { out.append(text.substring(markerStart)); break; }
            String inside = text.substring(contentStart, markerEnd).trim();
            int pipe = inside.indexOf('|');
            if (pipe >= 0) inside = inside.substring(0, pipe).trim();
            out.append(inside).append(' ');
            i = markerEnd + 3;
        }
        return out.toString();
    }

    private String bookCodeToName(String code) {
        Map<String, String> m = bookCodeMap();
        return m.getOrDefault(code.toUpperCase(Locale.ROOT), normalizeBookName(code));
    }

    private Map<String, String> bookCodeMap() {
        String[][] pairs = {
                {"GEN", "Genesis"}, {"EXO", "Exodus"}, {"LEV", "Leviticus"}, {"NUM", "Numbers"}, {"DEU", "Deuteronomy"},
                {"JOS", "Joshua"}, {"JDG", "Judges"}, {"RUT", "Ruth"}, {"1SA", "1 Samuel"}, {"2SA", "2 Samuel"},
                {"1KI", "1 Kings"}, {"2KI", "2 Kings"}, {"1CH", "1 Chronicles"}, {"2CH", "2 Chronicles"}, {"EZR", "Ezra"},
                {"NEH", "Nehemiah"}, {"EST", "Esther"}, {"JOB", "Job"}, {"PSA", "Psalms"}, {"PRO", "Proverbs"},
                {"ECC", "Ecclesiastes"}, {"SNG", "Song of Solomon"}, {"ISA", "Isaiah"}, {"JER", "Jeremiah"}, {"LAM", "Lamentations"},
                {"EZK", "Ezekiel"}, {"DAN", "Daniel"}, {"HOS", "Hosea"}, {"JOL", "Joel"}, {"AMO", "Amos"}, {"OBA", "Obadiah"},
                {"JON", "Jonah"}, {"MIC", "Micah"}, {"NAM", "Nahum"}, {"HAB", "Habakkuk"}, {"ZEP", "Zephaniah"}, {"HAG", "Haggai"},
                {"ZEC", "Zechariah"}, {"MAL", "Malachi"}, {"MAT", "Matthew"}, {"MRK", "Mark"}, {"LUK", "Luke"}, {"JHN", "John"},
                {"ACT", "Acts"}, {"ROM", "Romans"}, {"1CO", "1 Corinthians"}, {"2CO", "2 Corinthians"}, {"GAL", "Galatians"},
                {"EPH", "Ephesians"}, {"PHP", "Philippians"}, {"COL", "Colossians"}, {"1TH", "1 Thessalonians"}, {"2TH", "2 Thessalonians"},
                {"1TI", "1 Timothy"}, {"2TI", "2 Timothy"}, {"TIT", "Titus"}, {"PHM", "Philemon"}, {"HEB", "Hebrews"},
                {"JAS", "James"}, {"1PE", "1 Peter"}, {"2PE", "2 Peter"}, {"1JN", "1 John"}, {"2JN", "2 John"},
                {"3JN", "3 John"}, {"JUD", "Jude"}, {"REV", "Revelation"}
        };
        Map<String, String> m = new HashMap<>();
        for (String[] p : pairs) m.put(p[0], p[1]);
        return m;
    }

    private void importBibleCsv() {
        JFileChooser ch = new JFileChooser(new File("."));
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try {
            int count = 0;
            for (String line : Files.readAllLines(ch.getSelectedFile().toPath(), StandardCharsets.UTF_8)) {
                if (line.trim().isEmpty()) continue;
                List<String> c = parseCsv(line);
                if (c.size() < 4 || c.get(0).equalsIgnoreCase("Book")) continue;
                data.putVerse(new Verse(normalizeBookName(c.get(0)), Integer.parseInt(c.get(1).trim()), Integer.parseInt(c.get(2).trim()), c.get(3).trim()));
                count++;
            }
            saveData();
            refreshEverything();
            log("Imported " + count + " Bible CSV verses.");
        } catch (Exception ex) {
            showError("Bible CSV import failed", ex);
        }
    }

    private void importGreekCsv() {
        JFileChooser ch = new JFileChooser(new File("."));
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try {
            int count = 0;
            for (String line : Files.readAllLines(ch.getSelectedFile().toPath(), StandardCharsets.UTF_8)) {
                if (line.trim().isEmpty()) continue;
                List<String> c = parseCsv(line);
                if (c.size() < 5 || c.get(0).equalsIgnoreCase("Book")) continue;
                GreekEntry ge = new GreekEntry(normalizeBookName(c.get(0)), Integer.parseInt(c.get(1).trim()), Integer.parseInt(c.get(2).trim()), c.get(3), c.get(4));
                data.greek.put(ge.key(), ge);
                count++;
            }
            saveData();
            refreshEverything();
            log("Imported " + count + " Greek CSV entries.");
        } catch (Exception ex) {
            showError("Greek CSV import failed", ex);
        }
    }

    private List<String> parseCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean q = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (q && i + 1 < line.length() && line.charAt(i + 1) == '"') { sb.append('"'); i++; }
                else q = !q;
            } else if (c == ',' && !q) {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out;
    }

    private void importMorphGntZipOrFolder() {
        JFileChooser ch = new JFileChooser(new File("."));
        ch.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File f = ch.getSelectedFile();
        new Thread(() -> {
            try {
                int count = f.isDirectory() ? importMorphFolder(f) : importMorphGntZipBytes(Files.readAllBytes(f.toPath()));
                log("Imported " + count + " Greek verse entries.");
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("Greek import failed", ex);
            }
        }).start();
    }

    private int importMorphFolder(File dir) throws IOException {
        Map<String, List<String[]>> grouped = new TreeMap<>();
        collectMorphFiles(dir, grouped);
        int c = saveGreekGroups(grouped);
        saveData();
        return c;
    }

    private void collectMorphFiles(File dir, Map<String, List<String[]>> grouped) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.isDirectory()) collectMorphFiles(f, grouped);
            else if (f.getName().toLowerCase(Locale.ROOT).endsWith(".txt")) parseMorphText(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8), grouped);
        }
    }

    private int importMorphGntZipBytes(byte[] bytes) throws IOException {
        Map<String, List<String[]>> grouped = new TreeMap<>();
        try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                if (!e.isDirectory() && e.getName().toLowerCase(Locale.ROOT).endsWith(".txt")) {
                    parseMorphText(new String(readAll(zin), StandardCharsets.UTF_8), grouped);
                }
            }
        }
        int c = saveGreekGroups(grouped);
        saveData();
        return c;
    }

    private void parseMorphText(String txt, Map<String, List<String[]>> grouped) {
        for (String raw : txt.split("\\R")) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] cols = line.split("\\s+");
            if (cols.length < 6) continue;
            String loc = cols[0].replaceAll("[^0-9]", "");
            if (loc.length() < 6) continue;
            String book = ntBookByNumber(loc.substring(0, 2));
            if (book == null) continue;
            int chapter = Integer.parseInt(loc.substring(2, 4));
            int verse = Integer.parseInt(loc.substring(4, 6));
            String key = book + " " + chapter + ":" + verse;
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(cols);
        }
    }

    private int saveGreekGroups(Map<String, List<String[]>> grouped) {
        int count = 0;
        for (String key : grouped.keySet()) {
            List<String[]> rows = grouped.get(key);
            StringBuilder greek = new StringBuilder();
            StringBuilder details = new StringBuilder();
            for (String[] cols : rows) {
                String surface = cols.length > 5 ? cols[5] : cols[cols.length - 1];
                String lemma = cols.length > 6 ? cols[6] : "";
                greek.append(surface).append(' ');
                details.append(surface);
                if (!lemma.isEmpty()) details.append(" — lemma: ").append(lemma);
                if (cols.length > 2) details.append("; parse: ").append(cols[1]).append(" ").append(cols[2]);
                details.append("\n");
            }
            RefParts rp = parseRef(key);
            if (rp != null) {
                data.greek.put(key, new GreekEntry(rp.book, rp.chapter, rp.verse, greek.toString().trim(), details.toString().trim()));
                count++;
            }
        }
        return count;
    }

    private String ntBookByNumber(String n) {
        String[] books = {"Matthew", "Mark", "Luke", "John", "Acts", "Romans", "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians", "Philippians", "Colossians", "1 Thessalonians", "2 Thessalonians", "1 Timothy", "2 Timothy", "Titus", "Philemon", "Hebrews", "James", "1 Peter", "2 Peter", "1 John", "2 John", "3 John", "Jude", "Revelation"};
        try {
            int idx = Integer.parseInt(n) - 1;
            return idx >= 0 && idx < books.length ? books[idx] : null;
        } catch (Exception e) {
            return null;
        }
    }

    private void importLibraryText() {
        JFileChooser ch = new JFileChooser(new File("."));
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File f = ch.getSelectedFile();
        String title = JOptionPane.showInputDialog(this, "Title:", stripExt(f.getName()));
        if (title == null || title.trim().isEmpty()) return;
        try {
            String body = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            data.libraryDocs.add(new LibraryDoc(title.trim(), "Philosophy / Other", body));
            saveData();
            refreshEverything();
            log("Imported library document: " + title);
        } catch (Exception ex) {
            showError("TXT import failed", ex);
        }
    }

    private String stripExt(String s) {
        int i = s.lastIndexOf('.');
        return i > 0 ? s.substring(0, i) : s;
    }

    private void createTemplates() {
        try {
            Files.write(Paths.get("bible_template.csv"), Collections.singletonList("Book,Chapter,Verse,Text"), StandardCharsets.UTF_8);
            Files.write(Paths.get("greek_template.csv"), Collections.singletonList("Book,Chapter,Verse,GreekText,Details"), StandardCharsets.UTF_8);
            log("Created bible_template.csv and greek_template.csv");
        } catch (Exception ex) {
            showError("Template creation failed", ex);
        }
    }

    private void clearBibleText() {
        if (JOptionPane.showConfirmDialog(this, "Clear Bible text only? Greek, notes, library docs, and profiles stay saved.", "Clear Bible", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        data.bible.clear();
        selectedBook = "";
        selectedChapter = 1;
        saveData();
        refreshEverything();
        log("Bible text cleared.");
    }

    private void exportNotes() {
        JFileChooser ch = new JFileChooser(new File("study_notes_export.txt"));
        if (ch.showSaveDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(ch.getSelectedFile()), StandardCharsets.UTF_8))) {
            pw.println("Bible Study Export");
            pw.println("Profile: " + currentProfile.name);
            pw.println("Exported: " + new Date());
            pw.println();
            pw.println("CATEGORIES");
            pw.println("========================================");
            for (String c : currentProfile.categories.keySet()) {
                pw.println(c + " " + colorHex(colorForCategory(c)));
                pw.println(currentProfile.categories.get(c));
                pw.println();
            }
            pw.println("HIGHLIGHTS / NOTES");
            pw.println("========================================");
            for (TextAnnotation a : currentProfile.annotations) {
                pw.println("Source: " + a.sourceTitle);
                pw.println("Type: " + a.type);
                if (!a.category.isEmpty()) pw.println("Category: " + a.category);
                if (!a.target.isEmpty()) pw.println("Target: " + a.target);
                pw.println("Selected: " + a.selectedText);
                pw.println("Note: " + a.note);
                pw.println("----------------------------------------");
            }
            pw.println("QUESTIONS");
            pw.println("========================================");
            for (StudyQuestion q : currentProfile.questions) {
                pw.println(q.sourceTitle + " - " + (q.answered ? "Answered" : "Unfinished"));
                pw.println("Selected: " + q.selectedText);
                pw.println(q.question);
                pw.println("----------------------------------------");
            }
            log("Exported notes to " + ch.getSelectedFile().getAbsolutePath());
        } catch (Exception ex) {
            showError("Export failed", ex);
        }
    }

    private void backupNow() {
        try {
            if (!BACKUP_DIR.exists()) BACKUP_DIR.mkdirs();
            saveData();
            File dest = new File(BACKUP_DIR, "bible_reader_backup_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".ser");
            Files.copy(DATA_FILE.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
            log("Backup created: " + dest.getAbsolutePath());
        } catch (Exception ex) {
            showError("Backup failed", ex);
        }
    }

    private AppData loadData() {
        if (DATA_FILE.exists()) {
            try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(DATA_FILE))) {
                Object o = in.readObject();
                if (o instanceof AppData) return (AppData) o;
            } catch (Exception ignored) {}
        }
        return new AppData();
    }

    private void saveData() {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(DATA_FILE))) {
            out.writeObject(data);
        } catch (Exception ex) {
            System.err.println("Save failed: " + ex.getMessage());
        }
    }

    private void repairLoadedData() {
        if (data == null) data = new AppData();
        if (data.bible == null) data.bible = new TreeMap<>();
        if (data.profiles == null) data.profiles = new TreeMap<>();
        if (data.libraryDocs == null) data.libraryDocs = new ArrayList<>();
        if (data.greek == null) data.greek = new TreeMap<>();
        for (Profile p : data.profiles.values()) repairProfile(p);
    }

    private void repairProfile(Profile p) {
        if (p.annotations == null) p.annotations = new ArrayList<>();
        if (p.questions == null) p.questions = new ArrayList<>();
        if (p.categories == null) p.categories = new TreeMap<>();
        if (p.categoryColors == null) p.categoryColors = new TreeMap<>();
        for (String c : p.categories.keySet()) p.categoryColors.putIfAbsent(c, categoryBlue.getRGB());
        if (p.visitCounts == null) p.visitCounts = new HashMap<>();
        if (p.oldNotes != null && !p.oldNotes.isEmpty()) {
            for (StudyNote n : p.oldNotes) {
                TextAnnotation a = new TextAnnotation(n.refKey, n.refKey, 0, 0, "", n.type, n.category, n.body, "");
                p.annotations.add(a);
            }
            p.oldNotes.clear();
        }
    }

    private void showError(String title, Exception ex) {
        ex.printStackTrace();
        SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, title + ":\n" + ex.getMessage()));
        log(title + ": " + ex.getMessage());
    }

    private class CategoryCellRenderer extends DefaultListCellRenderer {
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
            JLabel label = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            String raw = value == null ? "" : value.toString();
            String cat = selectedCategoryNameFromListValue(raw);
            Color c = colorForCategory(cat);
            label.setText("■  " + raw);
            label.setForeground(isSelected ? list.getSelectionForeground() : c.darker());
            label.setBorder(new CompoundBorder(new MatteBorder(0, 7, 0, 0, c), new EmptyBorder(4, 8, 4, 4)));
            return label;
        }
    }

    private static class RefParts implements Serializable {
        private static final long serialVersionUID = 1L;
        String book;
        int chapter;
        int verse;
        RefParts(String b, int c, int v) { book = b; chapter = c; verse = v; }
        String key() { return book + " " + chapter + ":" + verse; }
    }

    private static class SearchResultParts {
        String type;
        String ref;
        String title;
        String extra;
        SearchResultParts(String type, String ref, String title, String extra) {
            this.type = type == null ? "" : type;
            this.ref = ref == null ? "" : ref;
            this.title = title == null ? "" : title;
            this.extra = extra == null ? "" : extra;
        }
    }

    private static class VerseMarker {
        int start;
        int end;
        int verse;
        VerseMarker(int s, int e, int v) { start = s; end = e; verse = v; }
    }

    private static class AppData implements Serializable {
        private static final long serialVersionUID = 30L;
        Map<String, TreeMap<Integer, TreeMap<Integer, Verse>>> bible = new TreeMap<>();
        Map<String, GreekEntry> greek = new TreeMap<>();
        Map<String, Profile> profiles = new TreeMap<>();
        List<LibraryDoc> libraryDocs = new ArrayList<>();

        void putVerse(Verse v) {
            bible.computeIfAbsent(v.book, k -> new TreeMap<>()).computeIfAbsent(v.chapter, k -> new TreeMap<>()).put(v.verse, v);
        }

        Set<Integer> getChapters(String book) {
            return bible.containsKey(book) ? bible.get(book).keySet() : new TreeSet<>();
        }

        Map<Integer, Verse> getVerses(String b, int c) {
            return bible.containsKey(b) && bible.get(b).containsKey(c) ? bible.get(b).get(c) : new TreeMap<>();
        }

        Verse findVerse(String key) {
            try {
                int colon = key.lastIndexOf(':');
                String left = key.substring(0, colon);
                int verse = Integer.parseInt(key.substring(colon + 1));
                int sp = left.lastIndexOf(' ');
                String book = left.substring(0, sp);
                int chapter = Integer.parseInt(left.substring(sp + 1));
                return getVerses(book, chapter).get(verse);
            } catch (Exception e) {
                return null;
            }
        }

        LibraryDoc findLibraryDoc(String title) {
            for (LibraryDoc d : libraryDocs) if (d.title.equals(title)) return d;
            return null;
        }

        int totalVerseCount() {
            int c = 0;
            for (String b : bible.keySet()) for (Integer ch : getChapters(b)) c += getVerses(b, ch).size();
            return c;
        }
    }

    private static class Profile implements Serializable {
        private static final long serialVersionUID = 30L;
        String name;
        List<TextAnnotation> annotations = new ArrayList<>();
        List<StudyQuestion> questions = new ArrayList<>();
        Map<String, String> categories = new TreeMap<>();
        Map<String, Integer> categoryColors = new TreeMap<>();
        Map<String, Integer> visitCounts = new HashMap<>();
        List<StudyNote> oldNotes = new ArrayList<>();
        Profile(String n) { name = n; }
    }

    private static class TextAnnotation implements Serializable {
        private static final long serialVersionUID = 30L;
        String id = UUID.randomUUID().toString();
        String sourceKey;
        String sourceTitle;
        int start;
        int end;
        String selectedText;
        String type;
        String category;
        String note;
        String target;
        Date created = new Date();

        TextAnnotation(String sourceKey, String sourceTitle, int start, int end, String selectedText, String type, String category, String note, String target) {
            this.sourceKey = sourceKey == null ? "" : sourceKey;
            this.sourceTitle = sourceTitle == null ? "" : sourceTitle;
            this.start = start;
            this.end = end;
            this.selectedText = selectedText == null ? "" : selectedText;
            this.type = type == null || type.isEmpty() ? "Note" : type;
            this.category = category == null ? "" : category;
            this.note = note == null ? "" : note;
            this.target = target == null ? "" : target;
        }
    }

    private static class GreekEntry implements Serializable {
        private static final long serialVersionUID = 30L;
        String book;
        int chapter;
        int verse;
        String greekText;
        String details;

        GreekEntry(String b, int c, int v, String g, String d) {
            book = b;
            chapter = c;
            verse = v;
            greekText = g == null ? "" : g;
            details = d == null ? "" : d;
        }

        String key() { return book + " " + chapter + ":" + verse; }
    }

    private static class Verse implements Serializable {
        private static final long serialVersionUID = 30L;
        String book;
        String text;
        int chapter;
        int verse;
        Verse(String b, int c, int v, String t) { book = b; chapter = c; verse = v; text = t; }
        String key() { return book + " " + chapter + ":" + verse; }
    }

    private static class StudyQuestion implements Serializable {
        private static final long serialVersionUID = 30L;
        String annotationId;
        String sourceTitle;
        String selectedText;
        String question;
        boolean answered = false;
        Date created = new Date();

        StudyQuestion(String annotationId, String sourceTitle, String selectedText, String question) {
            this.annotationId = annotationId;
            this.sourceTitle = sourceTitle;
            this.selectedText = selectedText;
            this.question = question;
        }
    }

    private static class StudyNote implements Serializable {
        private static final long serialVersionUID = 5L;
        String refKey;
        String type;
        String category;
        String body;
        Date created = new Date();
        StudyNote(String r, String t, String c, String b) { refKey = r; type = t; category = c == null ? "" : c; body = b; }
    }

    private static class LibraryDoc implements Serializable {
        private static final long serialVersionUID = 30L;
        String title;
        String type;
        String body;
        LibraryDoc(String t, String ty, String b) { title = t; type = ty; body = b; }
    }
}